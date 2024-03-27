package objectio

import (
	"context"
	"sds_use/bloomfilter/e_fileservice"
)

func LoadBFWithMeta(
	ctx context.Context,
	meta ObjectDataMeta,
	location Location,
	fs fileservice.FileService,
) (BloomFilter, error) {

	return nil, nil
}
