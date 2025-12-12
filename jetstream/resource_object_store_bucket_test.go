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
	"testing"
	"time"

	"github.com/hashicorp/terraform-plugin-sdk/v2/helper/resource"
	"github.com/hashicorp/terraform-plugin-sdk/v2/terraform"
	"github.com/nats-io/jsm.go"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const testObjectStoreBucket_basic = `
provider "jetstream" {
  servers = "%s"
}

resource "jetstream_object_store_bucket" "test" {
  name        = "TEST"
  description = "Test bucket"
  ttl         = 3600
  max_bytes   = 1048576
  replicas    = 1
  compression = true
  metadata = {
    "key1" = "value1"
  }
}
`

const testObjectStoreBucket_update = `
provider "jetstream" {
  servers = "%s"
}

resource "jetstream_object_store_bucket" "test" {
  name        = "TEST"
  description = "Updated description"
  ttl         = 7200
  max_bytes   = 2097152
  replicas    = 1
  compression = true
  metadata = {
    "key1" = "updated"
    "key2" = "new"
  }
}
`

func TestResourceObjectStoreBucket(t *testing.T) {
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

	resource.Test(t, resource.TestCase{
		ProviderFactories: testJsProviders,
		CheckDestroy:      testObjectStoreBucketDoesNotExist(t, mgr, "TEST"),
		Steps: []resource.TestStep{
			{
				Config: fmt.Sprintf(testObjectStoreBucket_basic, nc.ConnectedUrl()),
				Check: resource.ComposeTestCheckFunc(
					testObjectStoreBucketExist(t, mgr, "TEST"),
					resource.TestCheckResourceAttr("jetstream_object_store_bucket.test", "name", "TEST"),
					resource.TestCheckResourceAttr("jetstream_object_store_bucket.test", "description", "Test bucket"),
					resource.TestCheckResourceAttr("jetstream_object_store_bucket.test", "ttl", "3600"),
					resource.TestCheckResourceAttr("jetstream_object_store_bucket.test", "max_bytes", "1048576"),
					resource.TestCheckResourceAttr("jetstream_object_store_bucket.test", "replicas", "1"),
					resource.TestCheckResourceAttr("jetstream_object_store_bucket.test", "compression", "true"),
					resource.TestCheckResourceAttr("jetstream_object_store_bucket.test", "metadata.key1", "value1"),
				),
			},
		},
	})
}

func TestResourceObjectStoreBucketUpdate(t *testing.T) {
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

	resource.Test(t, resource.TestCase{
		ProviderFactories: testJsProviders,
		CheckDestroy:      testObjectStoreBucketDoesNotExist(t, mgr, "TEST"),
		Steps: []resource.TestStep{
			{
				Config: fmt.Sprintf(testObjectStoreBucket_basic, nc.ConnectedUrl()),
				Check: resource.ComposeTestCheckFunc(
					testObjectStoreBucketExist(t, mgr, "TEST"),
					resource.TestCheckResourceAttr("jetstream_object_store_bucket.test", "description", "Test bucket"),
					resource.TestCheckResourceAttr("jetstream_object_store_bucket.test", "ttl", "3600"),
					resource.TestCheckResourceAttr("jetstream_object_store_bucket.test", "max_bytes", "1048576"),
				),
			},
			{
				Config: fmt.Sprintf(testObjectStoreBucket_update, nc.ConnectedUrl()),
				Check: resource.ComposeTestCheckFunc(
					testObjectStoreBucketExist(t, mgr, "TEST"),
					resource.TestCheckResourceAttr("jetstream_object_store_bucket.test", "description", "Updated description"),
					resource.TestCheckResourceAttr("jetstream_object_store_bucket.test", "ttl", "7200"),
					resource.TestCheckResourceAttr("jetstream_object_store_bucket.test", "max_bytes", "2097152"),
					resource.TestCheckResourceAttr("jetstream_object_store_bucket.test", "metadata.key1", "updated"),
					resource.TestCheckResourceAttr("jetstream_object_store_bucket.test", "metadata.key2", "new"),
				),
			},
		},
	})
}

func TestResourceObjectStoreBucketExternalDeletion(t *testing.T) {
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
		CheckDestroy:      testObjectStoreBucketDoesNotExist(t, mgr, "TEST"),
		Steps: []resource.TestStep{
			{
				Config: fmt.Sprintf(testObjectStoreBucket_basic, nc.ConnectedUrl()),
				Check: resource.ComposeTestCheckFunc(
					testObjectStoreBucketExist(t, mgr, "TEST"),
					resource.TestCheckResourceAttr("jetstream_object_store_bucket.test", "name", "TEST"),
				),
			},
			{
				Config: fmt.Sprintf(testObjectStoreBucket_basic, nc.ConnectedUrl()),
				PreConfig: func() {
					err := js.DeleteObjectStore(ctx, "TEST")
					if err != nil {
						t.Fatalf("failed to externally delete bucket: %s", err)
					}
				},
				Check: resource.ComposeTestCheckFunc(
					testObjectStoreBucketExist(t, mgr, "TEST"),
					resource.TestCheckResourceAttr("jetstream_object_store_bucket.test", "name", "TEST"),
				),
			},
		},
	})
}

func testObjectStoreBucketDoesNotExist(t *testing.T, mgr *jsm.Manager, bucket string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		err := testObjectStoreBucketExist(t, mgr, bucket)(s)
		if err == nil {
			return fmt.Errorf("expected object store bucket %q to not exist", bucket)
		}

		return nil
	}
}

func testObjectStoreBucketExist(t *testing.T, mgr *jsm.Manager, bucket string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		known, err := mgr.IsKnownStream("OBJ_" + bucket)
		if err != nil {
			return err
		}

		if !known {
			return fmt.Errorf("object store bucket %s does not exist", bucket)
		}

		return nil
	}
}
