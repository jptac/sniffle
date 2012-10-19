Specs going for chunter
```
brand: "joyent/kvm",
alias: aslias as strin
quota: in GB
dataset(_uuid): uuid string
ram: in MB,
resolvers: [ip_1,ip_2] as array of strings
nics: [
    {
      nic_tag: "admin" as string
      ip: "172.16.1.5" as string
      netmask: "255.255.248.0" as String
      gateway: "172.16.0.1" as String
    }
  ]
ssh_keys: [ssh_key] as list of Strings
```

Package specs
```
package_name: name as String
ram: size in MB
quota: size in GB
```

Dataset Specs
```
spec_name: name as String
dataset: uuid as Sting
type: kvm/joyent
networks: [nic_tag]
```

examples:

```
c("apps/chunter/src/chunter.erl").

P = [{<<"name">>, <<"example package">>},{<<"ram">>, 1024},{<<"quota">>, 20}].

Dz = [{<<"name">>, <<"uuid!">>},
{<<"uuid">>, <<"uuid">>},
{<<"networks">>, [
[{<<"nic_tag">>, <<"admin">>},
{<<"ip">>, <<"172.16.1.5">>},
{<<"netmask">>, <<"255.255.248.0">>},
{<<"gateway">>, <<"172.16.0.1">>}]]}, {<<"type">>, <<"zone">>}].

Dk = [{<<"name">>, <<"uuid!">>}, 
{<<"uuid">>, <<"uuid">>}, 
{<<"networks">>, [
[{<<"nic_tag">>, <<"admin">>},
{<<"ip">>, <<"172.16.1.5">>},
{<<"netmask">>, <<"255.255.248.0">>},
{<<"gateway">>, <<"172.16.0.1">>}]]}, {<<"type">>, <<"kvm">>}].

chunter:generate_spec(P, Dz, []).
chunter:generate_spec(P, Dk,  [{<<"ssh_keys">>, <<"k">>}]).
```
 