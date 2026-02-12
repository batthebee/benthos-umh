// Copyright 2025 UMH Systems GmbH
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package opcua_plugin

import (
	"os"

	"github.com/gopcua/opcua/ua"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/redpanda-data/benthos/v4/public/service"
)

var _ = Describe("getBytesFromValue", func() {
	BeforeEach(func() {
		if os.Getenv("TEST_OPCUA_UNIT") == "" {
			Skip("Skipping OPC UA unit tests: TEST_OPCUA_UNIT not set")
		}
	})

	var (
		conn    *OPCUAConnection
		nodeDef NodeDef
	)

	BeforeEach(func() {
		logger := service.MockResources().Logger()
		conn = &OPCUAConnection{
			Log: logger,
		}
		nodeDef = NodeDef{
			NodeID:     ua.NewNumericNodeID(0, 1001),
			BrowseName: "TestNode",
		}
	})

	It("should return nil for non-OK status (StatusBadDataTypeIDUnknown)", func() {
		dataValue := &ua.DataValue{
			Status: ua.StatusBadDataTypeIDUnknown,
			Value:  ua.MustVariant(&ua.ExtensionObject{Value: nil}),
		}

		b, tagType := conn.getBytesFromValue(dataValue, nodeDef)
		Expect(b).To(BeNil())
		Expect(tagType).To(BeEmpty())
	})

	It("should return nil for any non-OK status", func() {
		dataValue := &ua.DataValue{
			Status: ua.StatusBadNodeIDUnknown,
			Value:  ua.MustVariant(int32(42)),
		}

		b, tagType := conn.getBytesFromValue(dataValue, nodeDef)
		Expect(b).To(BeNil())
		Expect(tagType).To(BeEmpty())
	})

	It("should return bytes for OK status with int32 value", func() {
		dataValue := &ua.DataValue{
			Status: ua.StatusOK,
			Value:  ua.MustVariant(int32(42)),
		}

		b, tagType := conn.getBytesFromValue(dataValue, nodeDef)
		Expect(b).To(Equal([]byte("42")))
		Expect(tagType).To(Equal("number"))
	})

	It("should return bytes for OK status with float64 value", func() {
		dataValue := &ua.DataValue{
			Status: ua.StatusOK,
			Value:  ua.MustVariant(float64(3.14)),
		}

		b, tagType := conn.getBytesFromValue(dataValue, nodeDef)
		Expect(b).To(Equal([]byte("3.14")))
		Expect(tagType).To(Equal("number"))
	})

	It("should return bytes for OK status with string value", func() {
		dataValue := &ua.DataValue{
			Status: ua.StatusOK,
			Value:  ua.MustVariant("hello"),
		}

		b, tagType := conn.getBytesFromValue(dataValue, nodeDef)
		Expect(b).To(Equal([]byte("hello")))
		Expect(tagType).To(Equal("string"))
	})

	It("should return bytes for OK status with bool value", func() {
		dataValue := &ua.DataValue{
			Status: ua.StatusOK,
			Value:  ua.MustVariant(true),
		}

		b, tagType := conn.getBytesFromValue(dataValue, nodeDef)
		Expect(b).To(Equal([]byte("true")))
		Expect(tagType).To(Equal("bool"))
	})

	It("should return nil when variant is nil", func() {
		dataValue := &ua.DataValue{
			Status: ua.StatusOK,
			Value:  nil,
		}

		b, tagType := conn.getBytesFromValue(dataValue, nodeDef)
		Expect(b).To(BeNil())
		Expect(tagType).To(BeEmpty())
	})

	It("should return nil for OK status with ExtensionObject with nil value", func() {
		dataValue := &ua.DataValue{
			Status: ua.StatusOK,
			Value:  ua.MustVariant(&ua.ExtensionObject{Value: nil}),
		}

		b, tagType := conn.getBytesFromValue(dataValue, nodeDef)
		Expect(b).To(BeNil())
		Expect(tagType).To(BeEmpty())
	})

	It("should return nil for OK status with array of ExtensionObjects with nil values", func() {
		extObjs := []*ua.ExtensionObject{
			{Value: nil},
			{Value: nil},
			{Value: nil},
		}
		dataValue := &ua.DataValue{
			Status: ua.StatusOK,
			Value:  ua.MustVariant(extObjs),
		}

		b, tagType := conn.getBytesFromValue(dataValue, nodeDef)
		Expect(b).To(BeNil())
		Expect(tagType).To(BeEmpty())
	})
})
