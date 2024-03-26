package objectio

import (
	"context"
	"sds_use/bloomfilter/fileservice"
)

func LoadBFWithMeta(
	ctx context.Context,
	meta ObjectDataMeta,
	location Location,
	fs fileservice.FileService,
) (BloomFilter, error) {

	return nil, nil
}
