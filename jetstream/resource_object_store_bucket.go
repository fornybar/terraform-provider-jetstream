// Copyright 2025 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package jetstream

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/validation"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func resourceObjectStoreBucket() *schema.Resource {
	return &schema.Resource{
		Create: resourceObjectStoreBucketCreate,
		Read:   resourceObjectStoreBucketRead,
		Update: resourceObjectStoreBucketUpdate,
		Delete: resourceObjectStoreBucketDelete,
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},

		Schema: map[string]*schema.Schema{
			"name": {
				Type:         schema.TypeString,
				Description:  "The name of the Object Store bucket",
				Required:     true,
				ForceNew:     true,
				ValidateFunc: validation.StringMatch(bucketNameRegex, "bucket name must match ^[A-Za-z0-9_-]+$"),
			},
			"description": {
				Type:        schema.TypeString,
				Description: "Contains additional information about this bucket",
				Optional:    true,
				ForceNew:    false,
			},
			"ttl": {
				Type:         schema.TypeInt,
				Description:  "How many seconds objects will be kept in the bucket",
				Optional:     true,
				ForceNew:     false,
				Default:      0,
				ValidateFunc: validation.IntAtLeast(0),
			},
			"max_bytes": {
				Type:         schema.TypeInt,
				Description:  "Maximum size of the entire bucket",
				Default:      -1,
				Optional:     true,
				ForceNew:     false,
				ValidateFunc: validation.IntAtLeast(-1),
			},
			"storage": {
				Type:             schema.TypeString,
				Description:      "Storage backend to use (file or memory)",
				Default:          "file",
				Optional:         true,
				ForceNew:         true,
				ValidateDiagFunc: validateStorageTypeString(),
			},
			"replicas": {
				Type:         schema.TypeInt,
				Description:  "Number of cluster replicas to store",
				Default:      1,
				Optional:     true,
				ForceNew:     false,
				ValidateFunc: validation.All(validation.IntAtLeast(1), validation.IntAtMost(5)),
			},
			"placement_cluster": {
				Type:        schema.TypeString,
				Description: "Place the bucket in a specific cluster, influenced by placement_tags",
				Default:     "",
				Optional:    true,
				ForceNew:    true,
			},
			"placement_tags": {
				Type:        schema.TypeList,
				Description: "Place the bucket only on servers with these tags",
				Optional:    true,
				ForceNew:    true,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
			},
			"compression": {
				Type:        schema.TypeBool,
				Description: "Enable compression for the bucket (not compatible with memory storage)",
				Default:     false,
				Optional:    true,
				ForceNew:    false,
			},
			"metadata": {
				Type:        schema.TypeMap,
				Description: "Metadata for the bucket",
				Optional:    true,
				ForceNew:    false,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
			},
		},
	}
}

func resourceObjectStoreBucketCreate(d *schema.ResourceData, m any) error {
	nc, err := getConnection(d, m)
	if err != nil {
		return err
	}
	defer nc.Close()

	name := d.Get("name").(string)
	description := d.Get("description").(string)
	ttl := d.Get("ttl").(int)
	maxBytes := d.Get("max_bytes").(int)
	storageStr := d.Get("storage").(string)
	replicas := d.Get("replicas").(int)
	compression := d.Get("compression").(bool)

	var storage jetstream.StorageType
	switch storageStr {
	case "file":
		storage = jetstream.FileStorage
	case "memory":
		storage = jetstream.MemoryStorage
	}

	if compression && storage == jetstream.MemoryStorage {
		return fmt.Errorf("compression is not compatible with memory storage")
	}

	var placement *jetstream.Placement
	c, ok := d.GetOk("placement_cluster")
	if ok {
		placement = &jetstream.Placement{Cluster: c.(string)}
		pt, ok := d.GetOk("placement_tags")
		if ok {
			ts := pt.([]any)
			var tags = make([]string, len(ts))
			for i, tag := range ts {
				tags[i] = tag.(string)
			}
			placement.Tags = tags
		}
	}

	var metadata map[string]string
	if md, ok := d.GetOk("metadata"); ok {
		metadata = make(map[string]string)
		for k, v := range md.(map[string]any) {
			metadata[k] = v.(string)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}

	known, err := js.ObjectStore(ctx, name)
	if known != nil {
		return fmt.Errorf("object store bucket %s already exists", name)
	} else if err != nil {
		if !errors.Is(err, jetstream.ErrBucketNotFound) {
			return fmt.Errorf("failed to load object store bucket: %s", err)
		}
	}

	_, err = js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{
		Bucket:      name,
		Description: description,
		TTL:         time.Duration(ttl) * time.Second,
		MaxBytes:    int64(maxBytes),
		Storage:     storage,
		Replicas:    replicas,
		Placement:   placement,
		Compression: compression,
		Metadata:    metadata,
	})
	if err != nil {
		return err
	}

	d.SetId(fmt.Sprintf("JETSTREAM_OBJ_%s", name))

	return resourceObjectStoreBucketRead(d, m)
}

func resourceObjectStoreBucketRead(d *schema.ResourceData, m any) error {
	name, err := parseObjectStoreBucketID(d.Id())
	if err != nil {
		return err
	}

	nc, err := getConnection(d, m)
	if err != nil {
		return err
	}
	defer nc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}

	bucket, err := js.ObjectStore(ctx, name)
	if err != nil {
		if errors.Is(err, jetstream.ErrBucketNotFound) {
			d.SetId("")
			return nil
		}
		return err
	}

	status, err := bucket.Status(ctx)
	if err != nil {
		return err
	}

	d.Set("name", status.Bucket())
	d.Set("description", status.Description())
	d.Set("ttl", int(status.TTL().Seconds()))
	d.Set("replicas", status.Replicas())
	d.Set("compression", status.IsCompressed())

	metadata := make(map[string]string)
	if status.Metadata() != nil {
		for k, v := range status.Metadata() {
			if !strings.HasPrefix(k, "_nats.") {
				metadata[k] = v
			}
		}
	}
	d.Set("metadata", metadata)

	switch status.Storage() {
	case jetstream.FileStorage:
		d.Set("storage", "file")
	case jetstream.MemoryStorage:
		d.Set("storage", "memory")
	}

	streamName := fmt.Sprintf("OBJ_%s", name)
	stream, err := js.Stream(ctx, streamName)
	if err != nil {
		return err
	}
	si := stream.CachedInfo()

	d.Set("max_bytes", si.Config.MaxBytes)

	if si.Config.Placement != nil {
		d.Set("placement_cluster", si.Config.Placement.Cluster)
		d.Set("placement_tags", si.Config.Placement.Tags)
	}

	return nil
}

func resourceObjectStoreBucketUpdate(d *schema.ResourceData, m any) error {
	name := d.Get("name").(string)

	nc, err := getConnection(d, m)
	if err != nil {
		return err
	}
	defer nc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}

	bucket, err := js.ObjectStore(ctx, name)
	if err != nil {
		return err
	}

	status, err := bucket.Status(ctx)
	if err != nil {
		return err
	}

	description := d.Get("description").(string)
	ttl := d.Get("ttl").(int)
	maxBytes := d.Get("max_bytes").(int)
	replicas := d.Get("replicas").(int)
	compression := d.Get("compression").(bool)

	if compression && status.Storage() == jetstream.MemoryStorage {
		return fmt.Errorf("compression is not compatible with memory storage")
	}

	streamName := fmt.Sprintf("OBJ_%s", name)
	stream, err := js.Stream(ctx, streamName)
	if err != nil {
		return err
	}
	placement := stream.CachedInfo().Config.Placement

	var metadata map[string]string
	if md, ok := d.GetOk("metadata"); ok {
		metadata = make(map[string]string)
		for k, v := range md.(map[string]any) {
			metadata[k] = v.(string)
		}
	}

	_, err = js.CreateOrUpdateObjectStore(ctx, jetstream.ObjectStoreConfig{
		Bucket:      name,
		Description: description,
		TTL:         time.Duration(ttl) * time.Second,
		MaxBytes:    int64(maxBytes),
		Storage:     status.Storage(),
		Replicas:    replicas,
		Placement:   placement,
		Compression: compression,
		Metadata:    metadata,
	})
	if err != nil {
		return err
	}

	return resourceObjectStoreBucketRead(d, m)
}

func resourceObjectStoreBucketDelete(d *schema.ResourceData, m any) error {
	name := d.Get("name").(string)

	nc, err := getConnection(d, m)
	if err != nil {
		return err
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err = js.DeleteObjectStore(ctx, name)
	if err == nats.ErrStreamNotFound || errors.Is(err, jetstream.ErrBucketNotFound) {
		return nil
	} else if err != nil {
		return err
	}

	return nil
}
