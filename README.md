
# Dev setup:

```
S=/root/sniffle/_build/default/rel/sniffle
D=/opt/local/fifo-sniffle
for i in `ls $S`; do ln -s $S/$i $D/$i; done
rm -r $D/etc
cp -r $S/etc $D
```
