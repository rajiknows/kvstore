package internal

import "errors"

// this is the Basic interface of a kv-store
type Store interface{
	Get(key string)(val string, exists bool , err error)
	Put(key , val string )(error)
	Delete(key string)(error)
}


// the most basic form of a kv-store just a string  -> string map
type Kvstore map[string]string

func NewStore() Kvstore{
	return make(map[string]string)
}

func (kv Kvstore) Get(key string) (val string, exists bool , err error){
	val, exists = kv[key]
	return val, exists, nil
}


func (kv Kvstore) Put(key, val string)error{
	kv[key] = val
	return nil
}

func (kv Kvstore) Delete(key string)error{
	if  _, exists := kv[key] ; !exists{
		return errors.New("key not found")
	}
	delete(kv, key)
	return nil
}



