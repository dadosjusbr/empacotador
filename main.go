package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"

	csvLib "encoding/csv"

	"github.com/dadosjusbr/coletores/status"
	"github.com/dadosjusbr/proto/coleta"
	"github.com/dadosjusbr/proto/csv"
	"github.com/dadosjusbr/proto/pipeline"
	"github.com/frictionlessdata/datapackage-go/datapackage"
	"github.com/gocarina/gocsv"
	"google.golang.org/protobuf/encoding/prototext"
)

const (
	coletaFileName      = "coleta.csv"                  // hardcoded in datapackage_descriptor.json
	folhaFileName       = "contra_cheque.csv"           // hardcoded in datapackage_descriptor.json
	remuneracaoFileName = "remuneracao.csv"             // hardcoded in datapackage_descriptor.json
	packageFileName     = "datapackage_descriptor.json" // name of datapackage descriptor
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

	csvRc := coletaToCSV(er.Rc)

	buildedCSV, err := csvRc.Coleta.MarshalCSV()
	if err != nil {
		err = status.NewError(status.InvalidParameters, fmt.Errorf("error creating Coleta CSV sprintf method:%q", err))
		status.ExitFromError(err)
	}
	// Creating coleta csv
	f, err := os.Create(coletaFileName)
	if err != nil {
		log.Fatalln("failed to open file", err)
	}
	defer f.Close()
	w := csvLib.NewWriter(f)

	if err := w.WriteAll(buildPacoteCSV(buildedCSV)); err != nil { // calls Flush internally
		err = status.NewError(status.SystemError, fmt.Errorf("error writing folha de pagamento CSV:%q", err))
		status.ExitFromError(err)
	}

	// Creating contracheque csv
	if err := toCSVFile(csvRc.Folha.ContraCheque, folhaFileName); err != nil {
		err = status.NewError(status.InvalidParameters, fmt.Errorf("error creating Folha de pagamento CSV:%q", err))
		status.ExitFromError(err)
	}

	// Creating remuneracao csv
	if err := toCSVFile(csvRc.Remuneracoes.Remuneracao, remuneracaoFileName); err != nil {
		err = status.NewError(status.InvalidParameters, fmt.Errorf("error creating Remuneração CSV:%q", err))
		status.ExitFromError(err)
	}

	// Creating package descriptor.
	c, err := ioutil.ReadFile(packageFileName)
	if err != nil {
		err = status.NewError(status.InvalidParameters, fmt.Errorf("error reading datapackge_descriptor.json:%q", err))
		status.ExitFromError(err)
	}

	var desc map[string]interface{}
	if err := json.Unmarshal(c, &desc); err != nil {
		err = status.NewError(status.InvalidParameters, fmt.Errorf("error unmarshaling datapackage descriptor:%q", err))
		status.ExitFromError(err)
	}

	pkg, err := datapackage.New(desc, ".")
	if err != nil {
		err = status.NewError(status.InvalidParameters, fmt.Errorf("error create datapackage:%q", err))
		status.ExitFromError(err)
	}

	// Packing CSV and package descriptor.
	zipName := filepath.Join(outputPath, fmt.Sprintf("%s-%d-%d.zip", er.Rc.Coleta.Orgao, er.Rc.Coleta.Ano, er.Rc.Coleta.Mes))
	if err := pkg.Zip(zipName); err != nil {
		err = status.NewError(status.SystemError, fmt.Errorf("error zipping datapackage (%s):%q", zipName, err))
		status.ExitFromError(err)
	}

	// Sending results.
	er.Pr = &pipeline.ResultadoEmpacotamento{
		Pacote: zipName,
	}
	b, err := prototext.Marshal(&er)
	if err != nil {
		err = status.NewError(status.Unknown, fmt.Errorf("error marshalling packaging result (%s):%q", zipName, err))
		status.ExitFromError(err)
	}
	fmt.Printf("%s", b)
}

func coletaToCSV(rc *coleta.ResultadoColeta) csv.ResultadoColeta_CSV {
	var coleta csv.Coleta_CSV
	var remuneracoes csv.Remuneracoes_CSV
	var folha csv.FolhaDePagamento_CSV
	coleta.ChaveColeta = rc.Coleta.ChaveColeta
	coleta.Orgao = rc.Coleta.Orgao
	coleta.Mes = rc.Coleta.Mes
	coleta.Ano = rc.Coleta.Ano
	coleta.TimestampColeta = rc.Coleta.TimestampColeta
	coleta.RepositorioColetor = rc.Coleta.RepositorioColetor
	coleta.VersaoColetor = rc.Coleta.VersaoColetor
	coleta.DirColetor = rc.Coleta.DirColetor
	for _, v := range rc.Folha.ContraCheque {
		var contraCheque csv.ContraCheque_CSV
		contraCheque.IdContraCheque = v.IdContraCheque
		contraCheque.ChaveColeta = v.ChaveColeta
		contraCheque.Nome = v.Nome
		contraCheque.Matricula = v.Matricula
		contraCheque.Funcao = v.Funcao
		contraCheque.LocalTrabalho = v.LocalTrabalho
		contraCheque.Tipo = csv.ContraCheque_CSV_Tipo(v.Tipo)
		for _, k := range v.Remuneracoes.Remuneracao {
			var remuneracao csv.Remuneracao_CSV
			remuneracao.IdContraCheque = v.IdContraCheque
			remuneracao.ChaveColeta = v.ChaveColeta
			remuneracao.Natureza = csv.Remuneracao_CSV_Natureza(k.Natureza)
			remuneracao.Categoria = k.Categoria
			remuneracao.Item = k.Item
			remuneracao.Valor = k.Valor
			remuneracoes.Remuneracao = append(remuneracoes.Remuneracao, &remuneracao)
		}
		folha.ContraCheque = append(folha.ContraCheque, &contraCheque)
	}

	return csv.ResultadoColeta_CSV{Coleta: &coleta, Remuneracoes: &remuneracoes, Folha: &folha}
}

func buildPacoteCSV(s string) [][]string {
	var b [][]string
	a := strings.Split(s, "\n")
	b = append(b, strings.Split(a[0], ","))
	b = append(b, strings.Split(a[1], ","))
	return b
}

// ToCSVFile dumps the payroll into a file using the CSV format.
func toCSVFile(in interface{}, path string) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("error creating CSV file(%s):%q", path, err)
	}
	defer f.Close()
	return gocsv.MarshalFile(in, f)
}
