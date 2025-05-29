package main

import (
	"archive/zip"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"unicode"

	"github.com/dadosjusbr/datapackage"
	"github.com/dadosjusbr/proto/coleta"
	"github.com/dadosjusbr/proto/pipeline"
	"github.com/dadosjusbr/status"
	"golang.org/x/exp/slices"
	"golang.org/x/text/runes"
	"golang.org/x/text/transform"
	"golang.org/x/text/unicode/norm"
	"google.golang.org/protobuf/encoding/prototext"
)

func main() {
	outputPath := os.Getenv("OUTPUT_FOLDER")
	if outputPath == "" {
		outputPath = "./"
	}
	var er pipeline.ResultadoExecucao
	er.Rc = new(coleta.ResultadoColeta)

	erIN, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		status.ExitFromError(status.NewError(4, fmt.Errorf("error reading crawling result: %q", err)))
	}
	if err = prototext.Unmarshal(erIN, er.Rc); err != nil {
		status.ExitFromError(status.NewError(5, fmt.Errorf("error unmarshaling crawling resul from STDIN: %q\n\n %s ", err, string(erIN))))
	}

	csvRc, err := datapackage.NewResultadoColetaCSV_V2(er.Rc)
	if err != nil {
		err = status.NewError(status.SystemError, fmt.Errorf("error creating CSV from crawling result: %q", err))
		status.ExitFromError(err)
	}

	zipName := filepath.Join(outputPath, fmt.Sprintf("%s-%d-%d.zip", er.Rc.Coleta.Orgao, er.Rc.Coleta.Ano, er.Rc.Coleta.Mes))
	if err := datapackage.ZipV2(zipName, csvRc, true); err != nil {
		err = status.NewError(status.SystemError, fmt.Errorf("error zipping datapackage (%s):%q", zipName, err))
		status.ExitFromError(err)
	}

	if len(er.Rc.Coleta.Arquivos) > 0 {
		bkpZip := filepath.Join(outputPath, fmt.Sprintf("backup-%s-%d-%d.zip", er.Rc.Coleta.Orgao, er.Rc.Coleta.Ano, er.Rc.Coleta.Mes))
		err := zipFiles(bkpZip, outputPath, er.Rc.Coleta.Arquivos)
		if err != nil {
			err = status.NewError(status.SystemError, fmt.Errorf("error zipping backup files (%s):%q", bkpZip, err))
			status.ExitFromError(err)
		}
		for _, f := range er.Rc.Coleta.Arquivos {
			if err := os.Remove(f); err != nil {
				err = status.NewError(status.SystemError, fmt.Errorf("error removing backup file (%s):%q", f, err))
				status.ExitFromError(err)
			}
		}
		er.Rc.Coleta.Arquivos = []string{bkpZip}
	}

	desambiguacao_micro, err := getItems("desambiguacao_micro.json")
	if err != nil {
		status.ExitFromError(fmt.Errorf("error getting desambiguacao_micro: %w", err))
	}

	desambiguacao_macro, err := getItems("desambiguacao_macro.json")
	if err != nil {
		status.ExitFromError(fmt.Errorf("error getting desambiguacao_macro: %w", err))
	}

	remunerations, countCategories := categorizeRemunerations(er.Rc, desambiguacao_micro, desambiguacao_macro)

	remunerationsFile := filepath.Join(outputPath, "remuneracoes.csv")
	if err = toCSVFile(&remunerations, remunerationsFile); err != nil {
		log.Fatalf("Error dumps remuneration into file (%s) : %v", remunerationsFile, err)
	}

	remunerationsZip := filepath.Join(outputPath, fmt.Sprintf("remuneracoes-%s-%d-%d.zip", er.Rc.Coleta.Orgao, er.Rc.Coleta.Ano, er.Rc.Coleta.Mes))
	err = zipFiles(remunerationsZip, outputPath, []string{remunerationsFile})
	if err != nil {
		log.Fatalf("Error zipping remunerations file: %q", err)
	}

	// Removendo o resquício do arquivo das remunerações
	err = os.Remove(remunerationsFile)
	if err != nil {
		log.Fatalf("Error removing remunerations file: %q", err)
	}
	// Sending results.
	er.Pr = &pipeline.ResultadoEmpacotamento{
		Remuneracoes: &pipeline.RemuneracoesZip{
			ZipUrl:       remunerationsZip,
			NumDescontos: countCategories.Descontos,
			NumBase:      countCategories.Base,
			NumOutras:    countCategories.Outras,
		},
		Pacote: zipName,
	}

	b, err := prototext.Marshal(&er)
	if err != nil {
		err = status.NewError(status.Unknown, fmt.Errorf("error marshalling packaging result (%s):%q", zipName, err))
		status.ExitFromError(err)
	}
	fmt.Printf("%s", b)
}

func zipFiles(filename string, basePath string, files []string) error {
	newfile, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer newfile.Close()
	zipWriter := zip.NewWriter(newfile)
	defer zipWriter.Close()
	for _, file := range files {
		zipfile, err := os.Open(file)
		if err != nil {
			return err
		}
		defer zipfile.Close()
		info, err := zipfile.Stat()
		if err != nil {
			return err
		}
		header, err := zip.FileInfoHeader(info)
		if err != nil {
			return err
		}

		// Deflate is the compression method.
		header.Method = zip.Deflate
		t := strings.TrimPrefix(strings.TrimPrefix(file, basePath), "/")
		if filepath.Dir(t) != "." {
			header.Name = t
		}
		writer, err := zipWriter.CreateHeader(header)
		if err != nil {
			return err
		}
		_, err = io.Copy(writer, zipfile)
		if err != nil {
			return err
		}
	}
	return nil
}

func categorizeRemunerations(rc *coleta.ResultadoColeta, micro, macro map[string]string) ([]Remuneracao, Categoria) {
	var remunerations []Remuneracao
	var cat Categoria

	for _, c := range rc.Folha.ContraCheque {
		for _, r := range c.Remuneracoes.Remuneracao {
			// Erroneamente, nem todos os descontos estão vindo com valor negativo. Por isso, multiplicamos por -1.
			if r.Natureza == coleta.Remuneracao_D && r.Valor > 0 {
				r.Valor *= -1
			}
			/*Esses são os diferentes nomes que os órgãos dão para a remuneração base(se ignorarmos caracteres especiais);*/
			categories := []string{"subsidio", "cargo efetivo", "remuneracao basica", "remuneracao do cargo efetivo"}
			t := transform.Chain(norm.NFD,
				runes.Remove(runes.In(unicode.Mn)),
				norm.NFC,
				runes.Map(unicode.ToLower))
			// Ignorando os caracteres especiais da categoria
			result, _, _ := transform.String(t, strings.TrimSpace(r.Item))

			var category string
			dmicro, dmacro := "", ""

			// Definindo a categoria do contracheque
			if r.Natureza == coleta.Remuneracao_D {
				category = "descontos"
				cat.Descontos++
			} else if r.TipoReceita == coleta.Remuneracao_B || slices.Contains(categories, result) {
				category = "base"
				cat.Base++
			} else {
				category = "outras"
				cat.Outras++
				dmicro = micro[sanitizarItem(r.Item)]
				dmacro = macro[sanitizarItem(r.Item)]
			}
			remunerations = append(remunerations, Remuneracao{
				Ano:                      rc.Coleta.Ano,
				Mes:                      rc.Coleta.Mes,
				Orgao:                    rc.Coleta.Orgao,
				Nome:                     c.Nome,
				Matricula:                c.Matricula,
				Cargo:                    c.Funcao,
				Lotacao:                  c.LocalTrabalho,
				Valor:                    datapackage.CustomFloat32(r.Valor),
				DetalhamentoContracheque: r.Item,
				CategoriaContracheque:    category,
				DesambiguacaoMicro:       dmicro,
				DesambiguacaoMacro:       dmacro,
			})
		}
	}
	return remunerations, cat
}

// Sanitizando as rubricas e nomes:
// deixando-as em minúsculo, sem acentos, pontuações, caracteres especiais e espaços duplos
func sanitizarItem(item string) string {
	// Converte para minúsculas
	item = strings.ToLower(item)

	// Remove acentos
	t := transform.Chain(norm.NFD, runes.Remove(runes.In(unicode.Mn)), norm.NFC)
	item, _, _ = transform.String(t, item)

	// Remove pontuação
	item = strings.Map(func(r rune) rune {
		if strings.ContainsRune(".,;:!?-", r) {
			return -1
		}
		return r
	}, item)

	// Remove caracteres especiais
	re := regexp.MustCompile("[^a-zA-Z0-9 ]")
	item = re.ReplaceAllString(item, "")

	// Remove espaços duplos e espaços no início/final da string
	item = strings.Join(strings.Fields(item), " ")

	return item
}

// Realiza o download do json com as rubricas desambiguadas
func getItems(file string) (map[string]string, error) {
	// json com rubricas desambiguadas
	url := fmt.Sprintf("https://raw.githubusercontent.com/dadosjusbr/desambiguador/main/%s", file)

	res, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("error getting items from %s: %w", url, err)
	}

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response body: %w", err)
	}

	var itemJson map[string]string

	// unmarshall
	if err := json.Unmarshal(body, &itemJson); err != nil {
		return nil, fmt.Errorf("error unmarshalling json: %w", err)
	}

	return itemJson, nil
}
