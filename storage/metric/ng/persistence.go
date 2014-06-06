package storage_ng

import (
	"fmt"
	"os"
	"path"
	"strconv"
	//"github.com/prometheus/prometheus/storage/metric"

	//"github.com/prometheus/client_golang/prometheus"

	clientmodel "github.com/prometheus/client_golang/model"
)

type diskPersistence struct {
	basePath string
}

func NewDiskPersistence(basePath string) (Persistence, error) {
	err := os.MkdirAll(basePath, 0700)
	if err != nil {
		return nil, err
	}
	return &diskPersistence{
		basePath: basePath,
	}, nil
}

func (p *diskPersistence) dirForFingerprint(fp *clientmodel.Fingerprint) string {
	fpStr := fp.String()
	return fmt.Sprintf("%s/%c%c/%s", p.basePath, fpStr[0], fpStr[1], fpStr[2:])
}

// exists returns true when the given file or directory exists.
func exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}

	return false, err
}

func (p *diskPersistence) openChunkFile(fp *clientmodel.Fingerprint, c chunk) (*os.File, error) {
	dirname := p.dirForFingerprint(fp)
	ex, err := exists(dirname)
	if err != nil {
		return nil, err
	}
	if !ex {
		if err := os.MkdirAll(dirname, 0700); err != nil {
			return nil, err
		}
	}
	return os.Create(path.Join(dirname, strconv.FormatInt(c.firstTime().Unix(), 10)))
}

func (p *diskPersistence) Persist(fp *clientmodel.Fingerprint, c chunk) error {
	// 1. Open chunk file.
	f, err := p.openChunkFile(fp, c)
	if err != nil {
		return err
	}
	defer f.Close()
	// 2. Write chunk into file.
	return c.marshal(f)
	// 3. Close the file.
}
