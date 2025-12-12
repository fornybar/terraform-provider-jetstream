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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
	"github.com/nats-io/jsm.go"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const testObjectStoreObject_basic = `
provider "jetstream" {
  servers = "%s"
}

resource "jetstream_object_store_bucket" "test" {
  name = "TEST"
}

resource "jetstream_object_store_object" "test_object" {
  bucket = jetstream_object_store_bucket.test.name
  name   = "testfile"
  value  = "hello world"
}
`

const testObjectStoreObject_withMetadata = `
provider "jetstream" {
  servers = "%s"
}

resource "jetstream_object_store_bucket" "test" {
  name = "TEST"
}

resource "jetstream_object_store_object" "test_object" {
  bucket      = jetstream_object_store_bucket.test.name
  name        = "testfile"
  value       = "hello world"
  description = "A test object"
  headers = {
    "Content-Type" = "text/plain"
  }
  metadata = {
    "author" = "test"
    "version" = "1.0"
  }
}
`

func TestResourceObjectStoreObject(t *testing.T) {
	srv := createJSServer(t)
	defer srv.Shutdown()

	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("could not connect: %s", err)
	}
	defer nc.Close()

	mgr, err := jsm.New(nc)
	if err != nil {
		t.Fatalf("could not connect: %s", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("could not connect: %s", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	resource.Test(t, resource.TestCase{
		ProviderFactories: testJsProviders,
		CheckDestroy:      testObjectStoreObjectDoesNotExist(ctx, t, js, "TEST", "testfile"),
		Steps: []resource.TestStep{
			{
				Config: fmt.Sprintf(testObjectStoreObject_basic, nc.ConnectedUrl()),
				Check: resource.ComposeTestCheckFunc(
					testObjectStoreBucketExist(t, mgr, "TEST"),
					testObjectStoreObjectExist(ctx, t, js, "TEST", "testfile"),
					resource.TestCheckResourceAttr("jetstream_object_store_object.test_object", "bucket", "TEST"),
					resource.TestCheckResourceAttr("jetstream_object_store_object.test_object", "name", "testfile"),
					resource.TestCheckResourceAttr("jetstream_object_store_object.test_object", "value", "hello world"),
					resource.TestCheckResourceAttr("jetstream_object_store_object.test_object", "size", "11"),
					resource.TestCheckResourceAttrSet("jetstream_object_store_object.test_object", "digest"),
					resource.TestCheckResourceAttrSet("jetstream_object_store_object.test_object", "mtime"),
				),
			},
		},
	})
}

func TestResourceObjectStoreObjectUpdate(t *testing.T) {
	srv := createJSServer(t)
	defer srv.Shutdown()

	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("could not connect: %s", err)
	}
	defer nc.Close()

	mgr, err := jsm.New(nc)
	if err != nil {
		t.Fatalf("could not connect: %s", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("could not connect: %s", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	updateBasicConfig := strings.ReplaceAll(testObjectStoreObject_basic, "hello world", "goodbye world")

	resource.Test(t, resource.TestCase{
		ProviderFactories: testJsProviders,
		CheckDestroy:      testObjectStoreObjectDoesNotExist(ctx, t, js, "TEST", "testfile"),
		Steps: []resource.TestStep{
			{
				Config: fmt.Sprintf(testObjectStoreObject_basic, nc.ConnectedUrl()),
				Check: resource.ComposeTestCheckFunc(
					testObjectStoreBucketExist(t, mgr, "TEST"),
					testObjectStoreObjectExist(ctx, t, js, "TEST", "testfile"),
					resource.TestCheckResourceAttr("jetstream_object_store_object.test_object", "value", "hello world"),
					resource.TestCheckResourceAttr("jetstream_object_store_object.test_object", "size", "11"),
				),
			},
			{
				Config: fmt.Sprintf(updateBasicConfig, nc.ConnectedUrl()),
				Check: resource.ComposeTestCheckFunc(
					testObjectStoreBucketExist(t, mgr, "TEST"),
					testObjectStoreObjectExist(ctx, t, js, "TEST", "testfile"),
					resource.TestCheckResourceAttr("jetstream_object_store_object.test_object", "value", "goodbye world"),
					resource.TestCheckResourceAttr("jetstream_object_store_object.test_object", "size", "13"),
				),
			},
		},
	})
}

func TestResourceObjectStoreObjectWithMetadata(t *testing.T) {
	srv := createJSServer(t)
	defer srv.Shutdown()

	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("could not connect: %s", err)
	}
	defer nc.Close()

	mgr, err := jsm.New(nc)
	if err != nil {
		t.Fatalf("could not connect: %s", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("could not connect: %s", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	resource.Test(t, resource.TestCase{
		ProviderFactories: testJsProviders,
		CheckDestroy:      testObjectStoreObjectDoesNotExist(ctx, t, js, "TEST", "testfile"),
		Steps: []resource.TestStep{
			{
				Config: fmt.Sprintf(testObjectStoreObject_withMetadata, nc.ConnectedUrl()),
				Check: resource.ComposeTestCheckFunc(
					testObjectStoreBucketExist(t, mgr, "TEST"),
					testObjectStoreObjectExist(ctx, t, js, "TEST", "testfile"),
					resource.TestCheckResourceAttr("jetstream_object_store_object.test_object", "bucket", "TEST"),
					resource.TestCheckResourceAttr("jetstream_object_store_object.test_object", "name", "testfile"),
					resource.TestCheckResourceAttr("jetstream_object_store_object.test_object", "value", "hello world"),
					resource.TestCheckResourceAttr("jetstream_object_store_object.test_object", "description", "A test object"),
					resource.TestCheckResourceAttr("jetstream_object_store_object.test_object", "headers.Content-Type", "text/plain"),
					resource.TestCheckResourceAttr("jetstream_object_store_object.test_object", "metadata.author", "test"),
					resource.TestCheckResourceAttr("jetstream_object_store_object.test_object", "metadata.version", "1.0"),
				),
			},
		},
	})
}

func TestResourceObjectStoreObjectExternalDeletion(t *testing.T) {
	srv := createJSServer(t)
	defer srv.Shutdown()

	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("could not connect: %s", err)
	}
	defer nc.Close()

	mgr, err := jsm.New(nc)
	if err != nil {
		t.Fatalf("could not connect: %s", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("could not connect: %s", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	resource.Test(t, resource.TestCase{
		ProviderFactories: testJsProviders,
		CheckDestroy:      testObjectStoreObjectDoesNotExist(ctx, t, js, "TEST", "testfile"),
		Steps: []resource.TestStep{
			{
				Config: fmt.Sprintf(testObjectStoreObject_basic, nc.ConnectedUrl()),
				Check: resource.ComposeTestCheckFunc(
					testObjectStoreBucketExist(t, mgr, "TEST"),
					testObjectStoreObjectExist(ctx, t, js, "TEST", "testfile"),
					resource.TestCheckResourceAttr("jetstream_object_store_object.test_object", "name", "testfile"),
					resource.TestCheckResourceAttr("jetstream_object_store_object.test_object", "value", "hello world"),
				),
			},
			{
				Config: fmt.Sprintf(testObjectStoreObject_basic, nc.ConnectedUrl()),
				PreConfig: func() {
					store, err := js.ObjectStore(ctx, "TEST")
					if err != nil {
						t.Fatalf("failed to get object store for external deletion: %s", err)
					}
					err = store.Delete(ctx, "testfile")
					if err != nil {
						t.Fatalf("failed to externally delete object: %s", err)
					}
				},
				Check: resource.ComposeTestCheckFunc(
					testObjectStoreBucketExist(t, mgr, "TEST"),
					testObjectStoreObjectExist(ctx, t, js, "TEST", "testfile"),
					resource.TestCheckResourceAttr("jetstream_object_store_object.test_object", "name", "testfile"),
					resource.TestCheckResourceAttr("jetstream_object_store_object.test_object", "value", "hello world"),
				),
			},
		},
	})
}

func TestResourceObjectStoreObjectBucketExternalDeletion(t *testing.T) {
	srv := createJSServer(t)
	defer srv.Shutdown()

	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("could not connect: %s", err)
	}
	defer nc.Close()

	mgr, err := jsm.New(nc)
	if err != nil {
		t.Fatalf("could not connect: %s", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("could not connect: %s", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	resource.Test(t, resource.TestCase{
		ProviderFactories: testJsProviders,
		CheckDestroy:      testObjectStoreObjectDoesNotExist(ctx, t, js, "TEST", "testfile"),
		Steps: []resource.TestStep{
			{
				Config: fmt.Sprintf(testObjectStoreObject_basic, nc.ConnectedUrl()),
				Check: resource.ComposeTestCheckFunc(
					testObjectStoreBucketExist(t, mgr, "TEST"),
					testObjectStoreObjectExist(ctx, t, js, "TEST", "testfile"),
					resource.TestCheckResourceAttr("jetstream_object_store_object.test_object", "name", "testfile"),
					resource.TestCheckResourceAttr("jetstream_object_store_object.test_object", "value", "hello world"),
				),
			},
			{
				Config: fmt.Sprintf(testObjectStoreObject_basic, nc.ConnectedUrl()),
				PreConfig: func() {
					err := js.DeleteObjectStore(ctx, "TEST")
					if err != nil {
						t.Fatalf("failed to externally delete bucket: %s", err)
					}
				},
				Check: resource.ComposeTestCheckFunc(
					testObjectStoreBucketExist(t, mgr, "TEST"),
					testObjectStoreObjectExist(ctx, t, js, "TEST", "testfile"),
					resource.TestCheckResourceAttr("jetstream_object_store_object.test_object", "name", "testfile"),
					resource.TestCheckResourceAttr("jetstream_object_store_object.test_object", "value", "hello world"),
				),
			},
		},
	})
}

func testObjectStoreObjectDoesNotExist(ctx context.Context, t *testing.T, js jetstream.JetStream, bucket string, name string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		err := testObjectStoreObjectExist(ctx, t, js, bucket, name)(s)
		if err == nil {
			return fmt.Errorf("expected object %q in bucket %q to not exist", name, bucket)
		}

		return nil
	}
}

func testObjectStoreObjectExist(ctx context.Context, t *testing.T, js jetstream.JetStream, bucket string, name string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		store, err := js.ObjectStore(ctx, bucket)
		if err != nil {
			return err
		}

		_, err = store.GetInfo(ctx, name)
		if err != nil {
			return err
		}

		return nil
	}
}
