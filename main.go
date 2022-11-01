package main

import (
	"archive/zip"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

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

	if len(er.Rc.Coleta.Arquivos) > 0 {
		bkpZip := filepath.Join(outputPath, fmt.Sprintf("backup-%s-%d-%d.zip", er.Rc.Coleta.Orgao, er.Rc.Coleta.Ano, er.Rc.Coleta.Mes))
		err := zipFiles(bkpZip, outputPath, er.Rc.Coleta.Arquivos)
		if err != nil {
			err = status.NewError(status.SystemError, fmt.Errorf("error zipping backup files (%s):%q", bkpZip, err))
			status.ExitFromError(err)
		}
		er.Rc.Coleta.Arquivos = append(er.Rc.Coleta.Arquivos, bkpZip)
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
