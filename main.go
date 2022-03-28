package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/dadosjusbr/coletores/status"
	"github.com/dadosjusbr/datapackage"
	"github.com/dadosjusbr/proto/coleta"
	"github.com/dadosjusbr/proto/pipeline"
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

	csvRc := datapackage.NewResultadoColetaCSV(er.Rc)

	zipName := filepath.Join(outputPath, fmt.Sprintf("%s-%d-%d.zip", er.Rc.Coleta.Orgao, er.Rc.Coleta.Ano, er.Rc.Coleta.Mes))
	if err := datapackage.Zip(zipName, csvRc, true); err != nil {
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
