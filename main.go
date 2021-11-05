package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/dadosjusbr/coletores/status"
	"github.com/dadosjusbr/proto/coleta"
	"github.com/dadosjusbr/proto/pipeline"
	"github.com/frictionlessdata/datapackage-go/datapackage"
	"github.com/gocarina/gocsv"
	"google.golang.org/protobuf/encoding/prototext"
)

const (
	coletaFileName      = "coleta.csv"                  // hardcoded in datapackage_descriptor.json
	folhaFileName       = "contra_cheque.csv"           // hardcoded in datapackage_descriptor.json
	remuneracaoFileName = "remuneracao.csv"             // hardcoded in datapackage_descriptor.json
	metadadosFileName   = "metadados.csv"               // hardcoded in datapackage_descriptor.json
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

	// Creating coleta csv
	if err := toCSVFile(csvRc.Coleta, coletaFileName); err != nil {
		status.ExitFromError(err)
	}

	// Creating contracheque csv
	if err := toCSVFile(csvRc.Folha, folhaFileName); err != nil {
		err = status.NewError(status.InvalidParameters, fmt.Errorf("error creating Folha de pagamento CSV:%q", err))
		status.ExitFromError(err)
	}

	// Creating remuneracao csv
	if err := toCSVFile(csvRc.Remuneracoes, remuneracaoFileName); err != nil {
		err = status.NewError(status.InvalidParameters, fmt.Errorf("error creating Remuneração CSV:%q", err))
		status.ExitFromError(err)
	}

	// Creating metadata csv
	if err := toCSVFile(csvRc.Metadados, metadadosFileName); err != nil {
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

func coletaToCSV(rc *coleta.ResultadoColeta) *ResultadoColeta_CSV {
	var coleta Coleta_CSV
	var remuneracoes []*Remuneracao_CSV
	var folha []*ContraCheque_CSV

	coleta.ChaveColeta = rc.Coleta.ChaveColeta
	coleta.Orgao = rc.Coleta.Orgao
	coleta.Mes = rc.Coleta.Mes
	coleta.Ano = rc.Coleta.Ano
	coleta.TimestampColeta = rc.Coleta.TimestampColeta.AsTime()
	coleta.RepositorioColetor = rc.Coleta.RepositorioColetor
	coleta.VersaoColetor = rc.Coleta.VersaoColetor
	coleta.DirColetor = rc.Coleta.DirColetor

	var metadados Metadados_CSV
	metadados.ChaveColeta = rc.Coleta.ChaveColeta
	metadados.NaoRequerLogin = rc.Metadados.NaoRequerLogin
	metadados.NaoRequerCaptcha = rc.Metadados.NaoRequerCaptcha
	metadados.Acesso = rc.Metadados.Acesso.String()
	metadados.Extensao = rc.Metadados.Extensao.String()
	metadados.EstritamenteTabular = rc.Metadados.EstritamenteTabular
	metadados.FormatoConsistente = rc.Metadados.FormatoConsistente
	metadados.TemMatricula = rc.Metadados.TemMatricula
	metadados.TemLotacao = rc.Metadados.TemLotacao
	metadados.TemCargo = rc.Metadados.TemCargo
	metadados.DetalhamentoReceitaBase = rc.Metadados.ReceitaBase.String()
	metadados.DetalhamentoOutrasReceitas = rc.Metadados.OutrasReceitas.String()
	metadados.DetalhamentoDescontos = rc.Metadados.Despesas.String()

	for _, v := range rc.Folha.ContraCheque {
		var contraCheque ContraCheque_CSV
		contraCheque.IdContraCheque = v.IdContraCheque
		contraCheque.ChaveColeta = v.ChaveColeta
		contraCheque.Nome = v.Nome
		contraCheque.Matricula = v.Matricula
		contraCheque.Funcao = v.Funcao
		contraCheque.Ativo = v.Ativo
		contraCheque.LocalTrabalho = v.LocalTrabalho
		contraCheque.Tipo = v.Tipo.String()
		for _, k := range v.Remuneracoes.Remuneracao {
			var remuneracao Remuneracao_CSV
			remuneracao.IdContraCheque = v.IdContraCheque
			remuneracao.ChaveColeta = v.ChaveColeta
			remuneracao.Natureza = k.Natureza.String()
			remuneracao.Categoria = k.Categoria
			remuneracao.Item = k.Item
			remuneracao.Valor = k.Valor
			remuneracoes = append(remuneracoes, &remuneracao)
		}
		folha = append(folha, &contraCheque)
	}

	return &ResultadoColeta_CSV{
		Coleta:       append([]*Coleta_CSV{}, &coleta),
		Remuneracoes: remuneracoes,
		Folha:        folha,
		Metadados:    append([]*Metadados_CSV{}, &metadados),
	}
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
