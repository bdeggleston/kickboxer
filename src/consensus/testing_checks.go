package consensus

import (
	"launchpad.net/gocheck"
)

type instMapContainsKeyChecker struct {
	*gocheck.CheckerInfo
}

func (c *instMapContainsKeyChecker) Check(params []interface{}, names []string) (result bool, error string) {
	if len(params) != 2 {
		return false, "2 arguments required"
	}
	mapObj, ok := params[0].(*InstanceMap)
	if !ok {
		return false, "first argument is not an InstanceMap"
	}
	expectedKey, ok := params[1].(InstanceID)
	if !ok {
		return false, "second argument is not an InstanceID"
	}
	instance := mapObj.Get(expectedKey)
	if instance == nil {
		return false, ""
	}
	return true, ""
}

var instMapContainsKey gocheck.Checker = &instMapContainsKeyChecker{
	&gocheck.CheckerInfo{
		Name: "mapContainsKey",
		Params: []string{"map", "expected key"},
	},
}

type instIdSliceContainsChecker struct {
	*gocheck.CheckerInfo
}

func (c *instIdSliceContainsChecker) Check(params []interface{}, names []string) (result bool, error string) {
	if len(params) != 2 {
		return false, "2 arguments required"
	}
	sliceObj, ok := params[0].([]InstanceID)
	if !ok {
		return false, "first argument is not of type []InstanceID)"
	}
	expectedKey, ok := params[1].(InstanceID)
	if !ok {
		return false, "second argument is not an InstanceID"
	}
	for _, obj := range sliceObj {
		if obj == expectedKey {
			return true, ""
		}
	}
	return false, ""
}

var instIdSliceContains gocheck.Checker = &instIdSliceContainsChecker{
	&gocheck.CheckerInfo{
		Name: "sliceContains",
		Params: []string{"slice", "expected element"},
	},
}
