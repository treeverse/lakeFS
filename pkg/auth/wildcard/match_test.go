package wildcard_test

/*
 * MinIO Cloud Storage, (C) 2015, 2016 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import (
	"testing"

	"github.com/treeverse/lakefs/pkg/auth/wildcard"
)

// TestMatch - Tests validate the logic of wild card matching.
// `Match` supports '*' and '?' wildcards.
// Sample usage: In resource matching for bucket policy validation.
func TestMatch(t *testing.T) {
	tests := []struct {
		pattern string
		text    string
		matched bool
	}{
		{
			pattern: "*",
			text:    "s3:GetObject",
			matched: true,
		},
		{
			pattern: "",
			text:    "s3:GetObject",
			matched: false,
		},
		{
			pattern: "",
			text:    "",
			matched: true,
		},
		{
			pattern: "s3:*",
			text:    "s3:ListMultipartUploadParts",
			matched: true,
		},
		{
			pattern: "s3:ListBucketMultipartUploads",
			text:    "s3:ListBucket",
			matched: false,
		},
		{
			pattern: "s3:ListBucket",
			text:    "s3:ListBucket",
			matched: true,
		},
		{
			pattern: "s3:ListBucketMultipartUploads",
			text:    "s3:ListBucketMultipartUploads",
			matched: true,
		},
		{
			pattern: "my-bucket/oo*",
			text:    "my-bucket/oo",
			matched: true,
		},
		{
			pattern: "my-bucket/In*",
			text:    "my-bucket/India/Karnataka/",
			matched: true,
		},
		{
			pattern: "my-bucket/In*",
			text:    "my-bucket/Karnataka/India/",
			matched: false,
		},
		{
			pattern: "my-bucket/In*/Ka*/Ban",
			text:    "my-bucket/India/Karnataka/Ban",
			matched: true,
		},
		{
			pattern: "my-bucket/In*/Ka*/Ban",
			text:    "my-bucket/India/Karnataka/Ban/Ban/Ban/Ban/Ban",
			matched: true,
		},
		{
			pattern: "my-bucket/In*/Ka*/Ban",
			text:    "my-bucket/India/Karnataka/Area1/Area2/Area3/Ban",
			matched: true,
		},
		{
			pattern: "my-bucket/In*/Ka*/Ban",
			text:    "my-bucket/India/State1/State2/Karnataka/Area1/Area2/Area3/Ban",
			matched: true,
		},
		{
			pattern: "my-bucket/In*/Ka*/Ban",
			text:    "my-bucket/India/Karnataka/Bangalore",
			matched: false,
		},
		{
			pattern: "my-bucket/In*/Ka*/Ban*",
			text:    "my-bucket/India/Karnataka/Bangalore",
			matched: true,
		},
		{
			pattern: "my-bucket/*",
			text:    "my-bucket/India",
			matched: true,
		},
		{
			pattern: "my-bucket/oo*",
			text:    "my-bucket/odo",
			matched: false,
		},
		{
			pattern: "my-bucket?/abc*",
			text:    "mybucket/abc",
			matched: false,
		},
		{
			pattern: "my-bucket?/abc*",
			text:    "my-bucket1/abc",
			matched: true,
		},
		{
			pattern: "my-?-bucket/abc*",
			text:    "my--bucket/abc",
			matched: false,
		},
		{
			pattern: "my-?-bucket/abc*",
			text:    "my-1-bucket/abc",
			matched: true,
		},
		{
			pattern: "my-?-bucket/abc*",
			text:    "my-k-bucket/abc",
			matched: true,
		},
		{
			pattern: "my??bucket/abc*",
			text:    "mybucket/abc",
			matched: false,
		},
		{
			pattern: "my??bucket/abc*",
			text:    "my4abucket/abc",
			matched: true,
		},
		{
			pattern: "my-bucket?abc*",
			text:    "my-bucket/abc",
			matched: true,
		},
		{
			pattern: "my-bucket/abc?efg",
			text:    "my-bucket/abcdefg",
			matched: true,
		},
		{
			pattern: "my-bucket/abc?efg",
			text:    "my-bucket/abc/efg",
			matched: true,
		},
		{
			pattern: "my-bucket/abc????",
			text:    "my-bucket/abc",
			matched: false,
		},
		{
			pattern: "my-bucket/abc????",
			text:    "my-bucket/abcde",
			matched: false,
		},
		{
			pattern: "my-bucket/abc????",
			text:    "my-bucket/abcdefg",
			matched: true,
		},
		{
			pattern: "my-bucket/abc?",
			text:    "my-bucket/abc",
			matched: false,
		},
		{
			pattern: "my-bucket/abc?",
			text:    "my-bucket/abcd",
			matched: true,
		},
		{
			pattern: "my-bucket/abc?",
			text:    "my-bucket/abcde",
			matched: false,
		},
		{
			pattern: "my-bucket/mnop*?",
			text:    "my-bucket/mnop",
			matched: false,
		},
		{
			pattern: "my-bucket/mnop*?",
			text:    "my-bucket/mnopqrst/mnopqr",
			matched: true,
		},
		{
			pattern: "my-bucket/mnop*?",
			text:    "my-bucket/mnopqrst/mnopqrs",
			matched: true,
		},
		{
			pattern: "my-bucket/mnop*?",
			text:    "my-bucket/mnop",
			matched: false,
		},
		{
			pattern: "my-bucket/mnop*?",
			text:    "my-bucket/mnopq",
			matched: true,
		},
		{
			pattern: "my-bucket/mnop*?",
			text:    "my-bucket/mnopqr",
			matched: true,
		},
		{
			pattern: "my-bucket/mnop*?and",
			text:    "my-bucket/mnopqand",
			matched: true,
		},
		{
			pattern: "my-bucket/mnop*?and",
			text:    "my-bucket/mnopand",
			matched: false,
		},
		{
			pattern: "my-bucket/mnop*?and",
			text:    "my-bucket/mnopqand",
			matched: true,
		},
		{
			pattern: "my-bucket/mnop*?",
			text:    "my-bucket/mn",
			matched: false,
		},
		{
			pattern: "my-bucket/mnop*?",
			text:    "my-bucket/mnopqrst/mnopqrs",
			matched: true,
		},
		{
			pattern: "my-bucket/mnop*??",
			text:    "my-bucket/mnopqrst",
			matched: true,
		},
		{
			pattern: "my-bucket/mnop*qrst",
			text:    "my-bucket/mnopabcdegqrst",
			matched: true,
		},
		{
			pattern: "my-bucket/mnop*?and",
			text:    "my-bucket/mnopqand",
			matched: true,
		},
		{
			pattern: "my-bucket/mnop*?and",
			text:    "my-bucket/mnopand",
			matched: false,
		},
		{
			pattern: "my-bucket/mnop*?and?",
			text:    "my-bucket/mnopqanda",
			matched: true,
		},
		{
			pattern: "my-bucket/mnop*?and",
			text:    "my-bucket/mnopqanda",
			matched: false,
		},
		{
			pattern: "my-?-bucket/abc*",
			text:    "my-bucket/mnopqanda",
			matched: false,
		},
	}
	for i, tt := range tests {
		actualResult := wildcard.Match(tt.pattern, tt.text)
		if tt.matched != actualResult {
			t.Errorf("Match('%s', '%s') [%d] expected=%t, got=%t",
				tt.pattern, tt.text, i+1, tt.matched, actualResult)
		}
	}
}
