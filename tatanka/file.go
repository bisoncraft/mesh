package tatanka

import "os"

// atomicWriteFile writes data to a file atomically by writing to a temporary
// file first and then renaming it to the target path.
func atomicWriteFile(path string, data []byte) error {
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0600); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return err
	}
	return nil
}
