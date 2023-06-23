package main

import (
	"testing"

	"github.com/dadosjusbr/proto/coleta"
	"github.com/stretchr/testify/assert"
)

func TestCategorizeRemunerations(t *testing.T) {
	rc := new(coleta.ResultadoColeta)
	rc.Coleta = &coleta.Coleta{
		Orgao: "teste",
		Mes:   1,
		Ano:   2018,
	}
	rc.Folha = new(coleta.FolhaDePagamento)
	rc.Folha.ContraCheque = []*coleta.ContraCheque{
		&coleta.ContraCheque{
			Nome:      "",
			Matricula: "",
			Funcao:    "",
			Remuneracoes: &coleta.Remuneracoes{
				Remuneracao: []*coleta.Remuneracao{
					&coleta.Remuneracao{
						Natureza:    coleta.Remuneracao_R,
						TipoReceita: coleta.Remuneracao_B,
						Categoria:   "Contracheque",
						Item:        "Vencimentos / Subsídios",
						Valor:       39293.32,
					},
					&coleta.Remuneracao{
						Categoria: "Contracheque",
						Item:      "Subsídio",
						Valor:     33689.11,
					},
					&coleta.Remuneracao{
						Natureza:    coleta.Remuneracao_R,
						TipoReceita: coleta.Remuneracao_O,
						Categoria:   "Contracheque",
						Item:        "Férias",
						Valor:       13097.77,
					},
					&coleta.Remuneracao{
						Natureza:  coleta.Remuneracao_D,
						Categoria: "Contracheque",
						Item:      "Retenção por Teto Constitucional",
						Valor:     1507.93,
					},
				},
			},
		},
	}

	remunerations, countCategories := categorizeRemunerations(rc)

	assert.Equal(t, remunerations[0].CategoriaContracheque, "base")
	assert.Equal(t, remunerations[1].CategoriaContracheque, "base")
	assert.Equal(t, remunerations[2].CategoriaContracheque, "outras")
	assert.Equal(t, remunerations[3].CategoriaContracheque, "descontos")
	assert.Equal(t, countCategories.Base, int32(2))
	assert.Equal(t, countCategories.Descontos, int32(1))
	assert.Equal(t, countCategories.Outras, int32(1))
}
