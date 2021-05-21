// Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
// This software is licensed under a 3-clause BSD license. Please consult the
// LICENSE.md file distributed with the sources of this project regarding your
// rights to use or distribute this software.

package experiments

import (
	"flag"
	"fmt"
	"os/exec"
	"strings"
	"testing"

	"github.com/gvallee/go_software_build/pkg/app"
	"github.com/gvallee/go_util/pkg/util"
	"github.com/gvallee/validation_tool/pkg/platform"
)

var partition = flag.String("partition", "", "Name of Slurm partition to use to run the test")
var modules = flag.String("modules", "", "Comma-separated list of modules to use to run the test")
var runDir = flag.String("run-dir", "", "From where the test needs to be executed")
var mpiInstallDir = flag.String("mpi-dir", "", "MPI install directory to use to execute the test")

func setExperiment(t *testing.T) *Experiment {
	var err error
	dummyApp := new(app.Info)
	dummyApp.Name = "date"
	dummyApp.BinName = "date"
	dummyApp.BinPath, err = exec.LookPath("date")
	if err != nil {
		t.Fatalf("unable to find date binary: %s", err)
	}

	targetPlatform := new(platform.Info)
	targetPlatform.Name = *partition
	targetPlatform.MaxNumNodes = 2
	targetPlatform.MaxPPR = 1

	e := new(Experiment)
	e.App = dummyApp
	e.Platform = targetPlatform
	if *modules != "" {
		e.RequiredModules = strings.Split(*modules, ",")
	}
	e.RunDir = *runDir
	if !util.PathExists(e.RunDir) {
		t.Fatalf("%s does not exist", e.RunDir)
	}

	return e
}
func TestRunSingle(t *testing.T) {
	r := newRuntime()

	e := setExperiment(t)
	e.MPICfg = new(MPIConfig)
	e.MPICfg.BuildEnv.InstallDir = *mpiInstallDir
	if !util.PathExists(e.MPICfg.BuildEnv.InstallDir) {
		t.Fatalf("%s does not exist", e.MPICfg.BuildEnv.InstallDir)
	}
	err := e.Run(r)
	if err != nil {
		t.Fatalf("experiment failed: %s", err)
	}
	e.Wait()

	fmt.Printf("Output: %s\n", e.Result.ExecRes.Stdout)
}

/*
func TestRunMany(t *testing.T) {
	r := newRuntime()

	e := setExperiment(t)
	exps := new(Experiments)
	exps.MPICfg = new(MPIConfig)
	exps.MPICfg.BuildEnv.InstallDir = *mpiInstallDir
	if !util.PathExists(exps.MPICfg.BuildEnv.InstallDir) {
		t.Fatalf("%s does not exist", exps.MPICfg.BuildEnv.InstallDir)
	}

	for i := 0; i < 10; i++ {
		exps.List = append(exps.List, e)
	}

	pass := exps.Run(r)
	if !pass {
		t.Fatalf("unable to run experiments")
	}
}
*/
