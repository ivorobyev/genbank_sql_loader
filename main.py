import genbank_loader as gl
import time

start_time = time.time()
print('Start getting data from Genbank by given taxons')
with open('taxons_full_list.txt', 'r') as f:
    taxons = []
    for line in f:
        taxons.append(int(line))

for t in taxons:
    gl.put_genbank_data_to_db('genbank.db', t, 100)

print('Done {0}'.format(time.time() - start_time))