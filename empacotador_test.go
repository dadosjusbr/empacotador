package main

import (
	"encoding/csv"
	"io/ioutil"
	"os"
	"testing"

	"github.com/dadosjusbr/proto/coleta"
	"github.com/dadosjusbr/proto/pipeline"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/encoding/prototext"
)

func TestCategorizeRemunerations(t *testing.T) {
	var er pipeline.ResultadoExecucao
	er.Rc = new(coleta.ResultadoColeta)

	erIN, _ := ioutil.ReadFile("entrada_test.txt")
	if err := prototext.Unmarshal(erIN, er.Rc); err != nil {
		t.Fatalf("error unmarshaling crawling resul from STDIN: %q\n\n %s ", err, string(erIN))
	}

	remunerations, countCategories := categorizeRemunerations(er.Rc)

	remunerationFile, err := os.Open("remuneracoes_test.csv")
	if err != nil {
		t.Fatalf("error reading remunerations.csv: %q", err)
	}
	defer remunerationFile.Close()

	remuneration, err := csv.NewReader(remunerationFile).ReadAll()
	if err != nil {
		t.Fatalf("error reading remunerationFile: %q", err)
	}

	for i, r := range remuneration {
		assert.Equal(t, remunerations[i].DetalhamentoContracheque, r[7])
		assert.Equal(t, remunerations[i].CategoriaContracheque, r[8])
	}

	assert.Equal(t, countCategories["base"], 55)
	assert.Equal(t, countCategories["descontos"], 44)
	assert.Equal(t, countCategories["outras"], 66)
}
