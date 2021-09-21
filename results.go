package main

import (
	"fmt"
	"os"

	"github.com/dadosjusbr/proto/coleta"
	"github.com/gocarina/gocsv"
)

// ExecutionResult collects the results of the whole dadosjusbr execution pipeline.
type ExecutionResult struct {
	Pr PackagingResult        `json:"pr,omitempty"`
	Rc coleta.ResultadoColeta `json:"rc,omitempty"`
}

// ProcInfo stores information about a process execution.
//
// NOTE 1: It could be used by any process in the data consolidation pipeline (i.e. validation) and should not contain information specific to a step.
// NOTE 2: Due to storage restrictions, as of 04/2020, we are only going to store process information when there is a failure. That allow us to make the consolidation simpler by storing the full
// stdout, stderr and env instead of backing everything up and storing links.
type ProcInfo struct {
	Stdin      string   `json:"stdin" bson:"stdin,omitempty"`             // String containing the standard input of the process.
	Stdout     string   `json:"stdout" bson:"stdout,omitempty"`           // String containing the standard output of the process.
	Stderr     string   `json:"stderr" bson:"stderr,omitempty"`           // String containing the standard error of the process.
	Cmd        string   `json:"cmd" bson:"cmd,omitempty"`                 // Command that has been executed
	CmdDir     string   `json:"cmddir" bson:"cmdir,omitempty"`            // Local directory, in which the command has been executed
	ExitStatus int      `json:"status,omitempty" bson:"status,omitempty"` // Exit code of the process executed
	Env        []string `json:"env,omitempty" bson:"env,omitempty"`       // Copy of strings representing the environment variables in the form ke=value
}

// PackagingResult stores the result of the package step, which creates the datapackage.
type PackagingResult struct {
	ProcInfo ProcInfo `json:"procinfo,omitempty"` // Information about the process execution
	Package  string   `json:"package"`            // Local file path of the package created by the step
}

// ToCSVFile dumps the payroll into a file using the CSV format.
func ToCSVFile(in interface{}, path string) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("Error creating CSV file(%s):%q", path, err)
	}
	defer f.Close()
	return gocsv.MarshalFile(in, f)
}
