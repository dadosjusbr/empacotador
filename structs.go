package main

import (
	"time"
)

type ResultadoColeta_CSV struct {
	Coleta       []*Coleta_CSV
	Remuneracoes []*Remuneracao_CSV
	Folha        []*ContraCheque_CSV
	Metadados    []*Metadados_CSV
}

type Coleta_CSV struct {
	ChaveColeta        string    `csv:"chave_coleta"`
	Orgao              string    `csv:"orgao"`
	Mes                int32     `csv:"mes"`
	Ano                int32     `csv:"ano"`
	TimestampColeta    time.Time `csv:"timestamp_coleta"`
	RepositorioColetor string    `csv:"repositorio_coletor"`
	VersaoColetor      string    `csv:"versao_coletor"`
	DirColetor         string    `csv:"dir_coletor"`
}

type ContraCheque_CSV struct {
	IdContraCheque string `csv:"id_contra_cheque"`
	ChaveColeta    string `csv:"chave_coleta"`
	Nome           string `csv:"nome"`
	Matricula      string `csv:"matricula"`
	Funcao         string `csv:"funcao"`
	LocalTrabalho  string `csv:"local_trabalho"`
	Tipo           string `csv:"tipo"`
	Ativo          bool   `csv:"ativo"`
}

type Metadados_CSV struct {
	ChaveColeta                string `csv:"chave_coleta"`
	NaoRequerLogin             bool   `csv:"nao_requer_login"`             // É necessário login para coleta dos dados?
	NaoRequerCaptcha           bool   `csv:"nao_requer_captcha"`           // É necessário captcha para coleta dos dados?
	Acesso                     string `csv:"acesso"`                       // Conseguimos prever/construir uma URL com base no órgão/mês/ano que leve ao download do dado?
	Extensao                   string `csv:"extensao"`                     // Extensao do arquivo de dados, ex: CSV, JSON, XLS, etc
	EstritamenteTabular        bool   `csv:"estritamente_tabular"`         // Órgãos que disponibilizam dados limpos (tidy data)
	FormatoConsistente         bool   `csv:"formato_consistente"`          // Órgão alterou a forma de expor seus dados entre o mês em questão e o mês anterior?
	TemMatricula               bool   `csv:"tem_matricula"`                // Órgão disponibiliza matrícula do servidor?
	TemLotacao                 bool   `csv:"tem_lotacao"`                  // Órgão disponibiliza lotação do servidor?
	TemCargo                   bool   `csv:"tem_cargo"`                    // Órgão disponibiliza a função do servidor?
	DetalhamentoReceitaBase    string `csv:"detalhamento_receita_base"`    // Contra-cheque
	DetalhamentoOutrasReceitas string `csv:"detalhamento_outras_receitas"` // Inclui indenizações, direitos eventuais, diárias, etc
	DetalhamentoDescontos      string `csv:"detalhamento_descontos"`       // Inclui imposto de renda, retenção por teto e contribuição previdenciária
}

type Remuneracao_CSV struct {
	IdContraCheque string  `csv:"id_contra_cheque"`
	ChaveColeta    string  `csv:"chave_coleta"`
	Natureza       string  `csv:"natureza"`
	Categoria      string  `csv:"categoria"`
	Item           string  `csv:"item"`
	Valor          float64 `csv:"valor"`
}
