import pygit2
import tqdm

odb = pygit2.Odb()
o = pygit2.OdbBackendPack("/Users/tom/tmp/foo2/objects/")
odb.add_backend(o, 1)
print(odb.exists("0093d11549b1f1772dfbac91d53efa471725e329"))
# print(o.exists())
item = 0
for x in tqdm.tqdm(odb):
    item += 1
    print(x)
    raise Exception("stop")
