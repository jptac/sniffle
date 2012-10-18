Specs going for chunter
```
brand: "joyent/kvm",
alias: aslias as strin
quota: in GB
dataset(_uuid): uuid string
ram: in MB,
resolvers: [ip_1,ip_2] as arry of strings
nics: [
    {
      nic_tag: "admin" as string
      ip: "172.16.1.5" as string
      netmask: "255.255.248.0" as string
      gateway: "172.16.0.1" as string
    }
  ]
```

Package specs
```
name: name as String
ram: size in MB
quota: size in GB
```

Dataset Specs
```
name: name as String
networks: [nic_tag]
```