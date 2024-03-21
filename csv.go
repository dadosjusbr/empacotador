package main

import (
	"encoding/csv"
	"fmt"
	"os"

	"github.com/dadosjusbr/datapackage"
	"github.com/gocarina/gocsv"
)

type Remuneracao struct {
	Ano                      int32                     `csv:"ano" tableheader:"ano"`
	Mes                      int32                     `csv:"mes" tableheader:"mes"`
	Orgao                    string                    `csv:"orgao" tableheader:"orgao"`
	Nome                     string                    `csv:"nome" tableheader:"nome"`
	Matricula                string                    `csv:"matricula" tableheader:"matricula"`
	Cargo                    string                    `csv:"cargo" tableheader:"cargo"`
	Lotacao                  string                    `csv:"lotacao" tableheader:"lotacao"`
	DetalhamentoContracheque string                    `csv:"detalhamento_contracheque" tableheader:"detalhamento_contracheque"`
	CategoriaContracheque    string                    `csv:"categoria_contracheque" tableheader:"categoria_contracheque"`
	Valor                    datapackage.CustomFloat32 `csv:"valor" tableheader:"valor"`
}

type Categoria struct {
	Base      int32
	Outras    int32
	Descontos int32
}

func toCSVFile(in interface{}, path string) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("error creating CSV file(%s):%q", path, err)
	}
	defer f.Close()

	// Crie um novo escritor CSV com os separadores personalizados
	csvWriter := csv.NewWriter(f)
	csvWriter.Comma = ';'
	csvWriter.UseCRLF = true // Para garantir que os fins de linha estejam no formato correto

	return gocsv.MarshalCSV(in, csvWriter)
}
