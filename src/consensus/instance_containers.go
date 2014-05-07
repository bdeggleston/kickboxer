package consensus

import (
	"encoding/json"
	"sync"
)

//
type InstanceIDSet map[InstanceID]bool

func NewInstanceIDSet(ids []InstanceID) InstanceIDSet {
	s := make(InstanceIDSet, len(ids))
	for _, id := range ids {
		s[id] = true
	}
	return s
}

func NewSizedInstanceIDSet(size int) InstanceIDSet {
	s := make(InstanceIDSet, size)
	return s
}

func (i InstanceIDSet) Equal(o InstanceIDSet) bool {
	if len(i) != len(o) {
		return false
	}
	for key := range i {
		if !o[key] {
			return false
		}
	}
	return true
}

func (i InstanceIDSet) Union(o InstanceIDSet) InstanceIDSet {
	u := make(InstanceIDSet, (len(i)*3)/2)
	for k := range i {
		u[k] = true
	}
	for k := range o {
		u[k] = true
	}
	return u
}

func (i InstanceIDSet) Intersect(o InstanceIDSet) InstanceIDSet {
	var maxLen int
	if li, lo := len(i), len(o); li > lo {
		maxLen = li
	} else {
		maxLen = lo
	}
	n := make(InstanceIDSet, maxLen)
	for k := range i {
		if _, exists := o[k]; exists {
			n[k] = true
		}
	}

	return n
}


func (i InstanceIDSet) Add(ids ...InstanceID) {
	for _, id := range ids {
		i[id] = true
	}
}

func (i InstanceIDSet) Remove(ids ...InstanceID) {
	for _, id := range ids {
		delete(i, id)
	}
}

func (i InstanceIDSet) Combine(idSets ...InstanceIDSet) {
	for _, ids := range idSets {
		for id := range ids {
			i[id] = true
		}
	}
}

// returns a new set with all of the keys in i, that aren't in o
func (i InstanceIDSet) Difference(o InstanceIDSet) InstanceIDSet {
	s := NewInstanceIDSet([]InstanceID{})
	for key := range i {
		if !o.Contains(key) {
			s.Add(key)
		}
	}
	return s
}

// remove all the keys in o, from i, returns removed keys
func (i InstanceIDSet) Subtract(o InstanceIDSet) {
	for key := range o {
		delete(i, key)
	}
}

// remove all keys from i
func (i InstanceIDSet) Clear() {
	for key := range i {
		delete(i, key)
	}
}

func (i InstanceIDSet) Copy() InstanceIDSet {
	c := NewSizedInstanceIDSet(i.Size())
	for k := range i {
		c[k] = true
	}
	return c
}

func (i InstanceIDSet) Contains(id InstanceID) bool {
	_, exists := i[id]
	return exists
}

func (i InstanceIDSet) Size() int {
	return len(i)
}

func (i InstanceIDSet) List() []InstanceID {
	l := make([]InstanceID, 0, len(i))
	for k := range i {
		l = append(l, k)
	}
	return l
}

func (i InstanceIDSet) String() string {
	s := "{"
	n := 0
	for k := range i {
		if n > 0 {
			s += ", "
		}
		s += k.String()
		n++
	}
	s += "}"
	return s
}

func (i InstanceIDSet) MarshalJSON() ([]byte, error) {
	return json.Marshal(i.List())
}

type InstanceMap struct {
	instMap map[InstanceID]*Instance
	lock sync.RWMutex
}

func NewInstanceMap() *InstanceMap {
	return &InstanceMap{instMap: make(map[InstanceID]*Instance)}
}

func (i *InstanceMap) Add(instance *Instance) {
	i.lock.Lock()
	defer i.lock.Unlock()
	i.instMap[instance.InstanceID] = instance
}

func (i *InstanceMap) Get(iid InstanceID) *Instance {
	i.lock.RLock()
	defer i.lock.RUnlock()
	return i.instMap[iid]
}

// gets an existing instance matching the given instance's iid, or adds the
// given instance to the map. The initialize function, if not nil, is run
// on the instance if it's being added to the map, before the map locks
// are released
func (i *InstanceMap) GetOrSet(inst *Instance, initialize func(*Instance)) (*Instance, bool) {
	if instance := i.Get(inst.InstanceID); instance != nil {
		return instance, true
	}

	i.lock.Lock()
	defer i.lock.Unlock()

	instance := i.instMap[inst.InstanceID]
	existed := true
	if instance == nil {
		if initialize != nil {
			initialize(inst)
		}
		i.instMap[inst.InstanceID] = inst
		existed = false
		instance = inst
	}
	return instance, existed
}

func (i *InstanceMap) Len() int {
	i.lock.RLock()
	defer i.lock.RUnlock()
	return len(i.instMap)
}

func (i *InstanceMap) Remove(instance *Instance) {
	i.lock.Lock()
	defer i.lock.Unlock()
	delete(i.instMap, instance.InstanceID)
}

func (i *InstanceMap) RemoveID(id InstanceID) {
	i.lock.Lock()
	defer i.lock.Unlock()
	delete(i.instMap, id)
}

func (i *InstanceMap) ContainsID(id InstanceID) bool {
	i.lock.RLock()
	defer i.lock.RUnlock()
	_, exists := i.instMap[id]
	return exists
}

func (i *InstanceMap) Contains(instance *Instance) bool {
	i.lock.RLock()
	defer i.lock.RUnlock()
	return i.ContainsID(instance.InstanceID)
}

func (i *InstanceMap) InstanceIDs() []InstanceID {
	i.lock.RLock()
	defer i.lock.RUnlock()
	arr := make([]InstanceID, 0, len(i.instMap))
	for key := range i.instMap {
		arr = append(arr, key)
	}
	return arr
}

func (i *InstanceMap) Instances() []*Instance {
	i.lock.RLock()
	defer i.lock.RUnlock()
	arr := make([]*Instance, 0, len(i.instMap))
	for _, instance := range i.instMap {
		arr = append(arr, instance)
	}
	return arr
}

// adds the instances of the given instance ids to the given instance map, and returns it
// if the given map is nil, a new map is allocated and returned
func (i *InstanceMap) GetMap(imap map[InstanceID]*Instance, iids []InstanceID) map[InstanceID]*Instance {
	if imap == nil {
		imap = make(map[InstanceID]*Instance, len(iids))
	}

	i.lock.RLock()
	defer i.lock.RUnlock()

	for _, iid := range iids {
		imap[iid] = i.instMap[iid]
	}

	return imap
}


