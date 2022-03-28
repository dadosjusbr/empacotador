package main

import (
	"time"
)

type ResultadoColeta_CSV struct {
	Coleta       []Coleta_CSV
	Remuneracoes []Remuneracao_CSV
	Folha        []ContraCheque_CSV
	Metadados    []Metadados_CSV
}

type Coleta_CSV struct {
	ChaveColeta        string    `csv:"chave_coleta" tableheader:"chave_coleta"`
	Orgao              string    `csv:"orgao" tableheader:"orgao"`
	Mes                int32     `csv:"mes" tableheader:"mes"`
	Ano                int32     `csv:"ano" tableheader:"ano"`
	TimestampColeta    time.Time `csv:"timestamp_coleta" tableheader:"timestamp_coleta"`
	RepositorioColetor string    `csv:"repositorio_coletor" tableheader:"repositorio_coletor"`
	VersaoColetor      string    `csv:"versao_coletor" tableheader:"versao_coletor"`
	DirColetor         string    `csv:"dir_coletor" tableheader:"dir_coletor"`
}

type ContraCheque_CSV struct {
	IdContraCheque string `csv:"id_contra_cheque" tableheader:"id_contra_cheque"`
	ChaveColeta    string `csv:"chave_coleta" tableheader:"chave_coleta"`
	Nome           string `csv:"nome" tableheader:"nome"`
	Matricula      string `csv:"matricula" tableheader:"matricula"`
	Funcao         string `csv:"funcao" tableheader:"funcao"`
	LocalTrabalho  string `csv:"local_trabalho" tableheader:"local_trabalho"`
	Tipo           string `csv:"tipo" tableheader:"tipo"`
	Ativo          bool   `csv:"ativo" tableheader:"ativo"`
}

type Metadados_CSV struct {
	ChaveColeta                string  `csv:"chave_coleta" tableheader:"chave_coleta"`
	NaoRequerLogin             bool    `csv:"nao_requer_login" tableheader:"nao_requer_login"`                         // É necessário login para coleta dos dados?
	NaoRequerCaptcha           bool    `csv:"nao_requer_captcha" tableheader:"nao_requer_captcha"`                     // É necessário captcha para coleta dos dados?
	Acesso                     string  `csv:"acesso" tableheader:"acesso"`                                             // Conseguimos prever/construir uma URL com base no órgão/mês/ano que leve ao download do dado?
	Extensao                   string  `csv:"extensao" tableheader:"extensao"`                                         // Extensao do arquivo de dados, ex: CSV, JSON, XLS, etc
	EstritamenteTabular        bool    `csv:"estritamente_tabular" tableheader:"estritamente_tabular"`                 // Órgãos que disponibilizam dados limpos (tidy data)
	FormatoConsistente         bool    `csv:"formato_consistente" tableheader:"formato_consistente"`                   // Órgão alterou a forma de expor seus dados entre o mês em questão e o mês anterior?
	TemMatricula               bool    `csv:"tem_matricula" tableheader:"tem_matricula"`                               // Órgão disponibiliza matrícula do servidor?
	TemLotacao                 bool    `csv:"tem_lotacao" tableheader:"tem_lotacao"`                                   // Órgão disponibiliza lotação do servidor?
	TemCargo                   bool    `csv:"tem_cargo" tableheader:"tem_cargo"`                                       // Órgão disponibiliza a função do servidor?
	DetalhamentoReceitaBase    string  `csv:"detalhamento_receita_base" tableheader:"detalhamento_receita_base"`       // Contra-cheque
	DetalhamentoOutrasReceitas string  `csv:"detalhamento_outras_receitas" tableheader:"detalhamento_outras_receitas"` // Inclui indenizações, direitos eventuais, diárias, etc
	DetalhamentoDescontos      string  `csv:"detalhamento_descontos" tableheader:"detalhamento_descontos"`             // Inclui imposto de renda, retenção por teto e contribuição previdenciária
	IndiceCompletude           float32 `csv:"indice_completude" tableheader:"indice_completude"`                       // Componente do índice de transparência resultante da análise dos metadados relacionados a disponibilidade dos dados.
	IndiceFacilidade           float32 `csv:"indice_facilidade" tableheader:"indice_facilidade"`                       // Componente do índice de transparência resultante da análise dos metadados relacionados a dificuldade para acessar os dados que estão disponíveis.
	IndiceTransparencia        float32 `csv:"indice_transparencia" tableheader:"indice_transparencia"`                 // Nota final, calculada utilizada os componentes de disponibilidade e dificuldade.
}

type Remuneracao_CSV struct {
	IdContraCheque string  `csv:"id_contra_cheque" tableheader:"id_contra_cheque"`
	ChaveColeta    string  `csv:"chave_coleta" tableheader:"chave_coleta"`
	Natureza       string  `csv:"natureza" tableheader:"natureza"`
	Categoria      string  `csv:"categoria" tableheader:"categoria"`
	Item           string  `csv:"item" tableheader:"item"`
	Valor          float64 `csv:"valor" tableheader:"valor"`
}
