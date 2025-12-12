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
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/schema"
	"github.com/nats-io/nats.go/jetstream"
)

func resourceObjectStoreObject() *schema.Resource {
	return &schema.Resource{
		Create: resourceObjectStoreObjectCreate,
		Read:   resourceObjectStoreObjectRead,
		Update: resourceObjectStoreObjectUpdate,
		Delete: resourceObjectStoreObjectDelete,
		Importer: &schema.ResourceImporter{
			StateContext: schema.ImportStatePassthroughContext,
		},

		Schema: map[string]*schema.Schema{
			"bucket": {
				Type:        schema.TypeString,
				Description: "The name of the Object Store bucket",
				Required:    true,
				ForceNew:    true,
			},
			"name": {
				Type:        schema.TypeString,
				Description: "The name of the object",
				Required:    true,
				ForceNew:    true,
			},
			"value": {
				Type:        schema.TypeString,
				Description: "The text content of the object",
				Required:    true,
				ForceNew:    false,
			},
			"description": {
				Type:        schema.TypeString,
				Description: "Description of the object",
				Optional:    true,
				ForceNew:    false,
			},
			"headers": {
				Type:        schema.TypeMap,
				Description: "Headers for the object (single-valued only)",
				Optional:    true,
				ForceNew:    false,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
			},
			"metadata": {
				Type:        schema.TypeMap,
				Description: "Metadata for the object",
				Optional:    true,
				ForceNew:    false,
				Elem: &schema.Schema{
					Type: schema.TypeString,
				},
			},
			"digest": {
				Type:        schema.TypeString,
				Description: "The digest of the object content",
				Computed:    true,
			},
			"size": {
				Type:        schema.TypeInt,
				Description: "The size of the object in bytes",
				Computed:    true,
			},
			"mtime": {
				Type:        schema.TypeString,
				Description: "The modification time of the object",
				Computed:    true,
			},
		},
	}
}

func resourceObjectStoreObjectCreate(d *schema.ResourceData, m any) error {
	nc, err := getConnection(d, m)
	if err != nil {
		return err
	}
	defer nc.Close()

	bucket := d.Get("bucket").(string)
	name := d.Get("name").(string)
	value := d.Get("value").(string)
	description := d.Get("description").(string)

	js, err := jetstream.New(nc)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	store, err := js.ObjectStore(ctx, bucket)
	if err != nil {
		return err
	}

	meta := jetstream.ObjectMeta{
		Name:        name,
		Description: description,
	}

	if hdrs, ok := d.GetOk("headers"); ok {
		meta.Headers = make(map[string][]string)
		for k, v := range hdrs.(map[string]any) {
			meta.Headers[k] = []string{v.(string)}
		}
	}

	if md, ok := d.GetOk("metadata"); ok {
		meta.Metadata = make(map[string]string)
		for k, v := range md.(map[string]any) {
			meta.Metadata[k] = v.(string)
		}
	}

	_, err = store.Put(ctx, meta, bytes.NewReader([]byte(value)))
	if err != nil {
		return err
	}

	d.SetId(fmt.Sprintf("JETSTREAM_OBJ_%s_OBJECT_%s", bucket, name))

	return resourceObjectStoreObjectRead(d, m)
}

func resourceObjectStoreObjectRead(d *schema.ResourceData, m any) error {
	bucket, name, err := parseObjectStoreObjectID(d.Id())
	if err != nil {
		return err
	}

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

	store, err := js.ObjectStore(ctx, bucket)
	if err != nil {
		if errors.Is(err, jetstream.ErrBucketNotFound) {
			d.SetId("")
			return nil
		}
		return err
	}

	info, err := store.GetInfo(ctx, name)
	if err != nil {
		if errors.Is(err, jetstream.ErrObjectNotFound) {
			d.SetId("")
			return nil
		}
		return err
	}

	result, err := store.GetBytes(ctx, name)
	if err != nil {
		return err
	}

	d.Set("bucket", bucket)
	d.Set("name", info.Name)
	d.Set("value", string(result))
	d.Set("description", info.Description)
	d.Set("digest", info.Digest)
	d.Set("size", info.Size)
	d.Set("mtime", info.ModTime.Format(time.RFC3339))

	if info.Headers != nil {
		headers := make(map[string]string)
		for k, v := range info.Headers {
			if len(v) > 0 {
				headers[k] = v[0]
			}
		}
		d.Set("headers", headers)
	}

	if info.Metadata != nil {
		d.Set("metadata", info.Metadata)
	}

	return nil
}

func resourceObjectStoreObjectUpdate(d *schema.ResourceData, m any) error {
	bucket := d.Get("bucket").(string)

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

	store, err := js.ObjectStore(ctx, bucket)
	if err != nil {
		return err
	}

	name := d.Get("name").(string)
	value := d.Get("value").(string)
	description := d.Get("description").(string)

	meta := jetstream.ObjectMeta{
		Name:        name,
		Description: description,
	}

	if hdrs, ok := d.GetOk("headers"); ok {
		meta.Headers = make(map[string][]string)
		for k, v := range hdrs.(map[string]any) {
			meta.Headers[k] = []string{v.(string)}
		}
	}

	if md, ok := d.GetOk("metadata"); ok {
		meta.Metadata = make(map[string]string)
		for k, v := range md.(map[string]any) {
			meta.Metadata[k] = v.(string)
		}
	}

	_, err = store.Put(ctx, meta, bytes.NewReader([]byte(value)))
	if err != nil {
		return err
	}

	return resourceObjectStoreObjectRead(d, m)
}

func resourceObjectStoreObjectDelete(d *schema.ResourceData, m any) error {
	bucket := d.Get("bucket").(string)

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

	store, err := js.ObjectStore(ctx, bucket)
	if err != nil {
		if errors.Is(err, jetstream.ErrBucketNotFound) {
			return nil
		}
		return err
	}

	err = store.Delete(ctx, d.Get("name").(string))
	if err != nil {
		if errors.Is(err, jetstream.ErrObjectNotFound) {
			return nil
		}
		return err
	}

	return nil
}
