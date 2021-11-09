//
// Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
//
// See LICENSE.txt for license information
//

package label

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strings"

	"github.com/gvallee/go_util/pkg/util"
)

const (
	labelFileName = "labels.txt"
)

// FromFile loads all the labels from a label file. It returns a map where the key is the experiment's hash and the value the experiment's label.
func FromFile(filePath string, labels map[string]string) error {
	if !util.PathExists(filePath) {
		return fmt.Errorf("%s does not exist", filePath)
	}

	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		return err
	}

	for _, line := range strings.Split(string(data), "\n") {
		if line == "" {
			continue
		}

		tokens := strings.Split(line, " ")
		if len(tokens) != 2 {
			return fmt.Errorf("label.FromFile() - invalid format: %s", line)
		}
		labels[tokens[0]] = tokens[1]
	}
	return nil
}

func GetFilePath(dir string) string {
	return filepath.Join(dir, labelFileName)
}

func Save(dir string, hash string, label string) error {
	// Sanity checks
	if hash == "" {
		return fmt.Errorf("undefined hash")
	}
	if label == "" {
		return fmt.Errorf("undefined label")
	}

	labelFilePath := GetFilePath(dir)
	labels := make(map[string]string)

	if util.FileExists(labelFilePath) {
		err := FromFile(labelFilePath, labels)
		if err != nil {
			return err
		}
	}

	if _, ok := labels[hash]; ok {
		// Label is already there, nothing to do
		return nil
	}

	labels[hash] = label
	content := ""
	for h, l := range labels {
		content += h + " " + l + "\n"
	}
	err := ioutil.WriteFile(labelFilePath, []byte(content), 0644)
	if err != nil {
		return err
	}
	return nil
}

func FromPermutation(p string, nameMap map[string]string) (string, error) {
	extraMpiArgs := strings.Split(p, " ")
	var experimentLabels []string
	for _, a := range extraMpiArgs {
		vars := strings.Split(a, "=")
		if len(vars) != 2 {
			return "", fmt.Errorf("cannot extract label from %s", a)
		}
		experimentLabels = append(experimentLabels, nameMap[vars[0]]+vars[1])
	}
	return strings.Join(experimentLabels, "-"), nil
}
