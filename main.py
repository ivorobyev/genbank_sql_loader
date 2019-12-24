import genbank_loader as gl
import time

start_time = time.time()
print('Start getting data from Genbank by given taxons')
with open('taxons_full_list.txt', 'r') as f:
    taxons = []
    for line in f:
        taxons.append(int(line))

print('All taxons: {0}'.format(len(taxons)))
counter = 0
for t in taxons:
    gl.put_genbank_data_to_db('genbank.db', t, 170)
    counter += 1
    print('Processed {0} from {1} taxons'.format(counter, len(taxons)))
    print('----------------------')
print('Done {0}'.format(time.time() - start_time))