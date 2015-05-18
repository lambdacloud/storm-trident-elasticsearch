/*
 * Copyright (c) 2015. LambdaCloud
 * All rights reserved.
 */

package com.github.fhuss.storm.elasticsearch.state;

import storm.trident.state.map.IBackingMap;
import storm.trident.util.LRUMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * Modified from storm's {@ref storm.trident.state.map.CachedMap}. The different is this one will not update
 * unchanged state
 * @author sky4star
 */
public class ExtCachedMap<T> implements IBackingMap<T> {
  LRUMap<List<Object>, T> _cache;
  IBackingMap<T> _delegate;

  public ExtCachedMap(IBackingMap<T> delegate, int cacheSize) {
    _cache = new LRUMap<List<Object>, T>(cacheSize);
    _delegate = delegate;
  }

  @Override
  public List<T> multiGet(List<List<Object>> keys) {
    Map<List<Object>, T> results = new HashMap<List<Object>, T>();
    List<List<Object>> toGet = new ArrayList<List<Object>>();
    for (List<Object> key : keys) {
      if (_cache.containsKey(key)) {
        results.put(key, _cache.get(key));
      } else {
        toGet.add(key);
      }
    }

    List<T> fetchedVals = _delegate.multiGet(toGet);
    for (int i = 0; i < toGet.size(); i++) {
      List<Object> key = toGet.get(i);
      T val = fetchedVals.get(i);
      _cache.put(key, val);
      results.put(key, val);
    }

    List<T> ret = new ArrayList<T>(keys.size());
    for (List<Object> key : keys) {
      ret.add(results.get(key));
    }
    return ret;
  }

  @Override
  public void multiPut(List<List<Object>> keys, List<T> values) {
    // Check if the same before updating
    List<List<Object>> toUpdateKeys = new ArrayList<>();
    List<T> toUpdateValues = new ArrayList<>();
    for (int i = 0; i < keys.size(); i++) {
      List<Object> key = keys.get(i);
      T value = values.get(i);
      if (_cache.get(key) == null
          || !_cache.get(key).equals(value)) {
        toUpdateKeys.add(key);
        toUpdateValues.add(value);
      }
    }

    // Only cache and update the different ones
    if (toUpdateKeys.size() > 0 && toUpdateKeys.size() == toUpdateValues.size()) {
      cache(toUpdateKeys, toUpdateValues);
      _delegate.multiPut(toUpdateKeys, toUpdateValues);
    }
  }

  private void cache(List<List<Object>> keys, List<T> values) {
    for (int i = 0; i < keys.size(); i++) {
      _cache.put(keys.get(i), values.get(i));
    }
  }
}