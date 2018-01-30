# Python Onegeo Manager

## Example usage

```
>>> import onegeo_manager
```

Declare a OGC:WFS source

```
>>> src = onegeo_manager.Source('http://hostname/ows', 'wfs')
<onegeo_manager.protocol.wfs.Source object>
```

Get one (or more) FeatureType

```
>>> res = src.get_resources(['enter_a_valid_typename'])
[<onegeo_manager.protocol.wfs.Resource object>]
```

Create a indexation profile

```
>>> idx_profile = onegeo_manager.IndexProfile('my_index_profile', res[0])
<onegeo_manager.protocol.wfs.IndexProfile object>
```

Generate ElasticSearch mapping with default configuration

```
>>> idx_profile.generate_elastic_mapping()
{'my_index_profile': {'properties': { ...
```

Get data collection in a *generator*

```
>>> idx_profile.get_collection()
<generator object IndexProfile.get_collection>
```

Then, you can use the official Python low-level client `elasticsearch-py` to push index and data to your elasticsearch instance.
