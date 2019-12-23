import xml.etree.cElementTree as ET
import sqlite3
import sys
import time 
import requests
from multiprocessing import Pool

def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)
 
    return conn

def search_elem(elem, child):
    res = child.find(elem).text if child.find(elem) is not None else ''
    return res

def get_sequence_ids_for_taxon(taxon_id):
    row = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/elink.fcgi?dbfrom=taxonomy&db=nucleotide&id={0}&rettype=xml'
    row = row.format(taxon_id)
    resp = requests.get(row)
    tree = ET.ElementTree(ET.fromstring(resp.text))
    links = tree.getroot().findall('LinkSet/LinkSetDb/Link/Id')
    return list(map(lambda x: x.text, links))

def extract_from_xml(a):
    primary_accession = search_elem('GBSeq_primary-accession',a)
    created = search_elem('GBSeq_create-date',a)
    updated = search_elem('GBSeq_update-date',a)

    protein_list = a.findall("GBSeq_feature-table/GBFeature/GBFeature_quals/GBQualifier[GBQualifier_name='translation']")
    protein_seq = ''
    for prot in protein_list:
        protein_seq += prot.find('GBQualifier_value').text+' '
    protein_seq = protein_seq.strip().replace(' ',';')
        
    dna_seq = search_elem('GBSeq_sequence',a)
    
    if dna_seq == '' and protein_seq == '':
        return None

    taxon = a.find("GBSeq_feature-table/GBFeature/GBFeature_quals/GBQualifier[GBQualifier_name='db_xref']")
    taxon = taxon.find('GBQualifier_value').text.split(':')[1] if taxon is not None else ''

    mol_type = a.find("GBSeq_feature-table/GBFeature/GBFeature_quals/GBQualifier[GBQualifier_name='mol_type']")
    mol_type = mol_type.find('GBQualifier_value').text if mol_type is not None else ''

    source = search_elem('GBSeq_source', a)
    description = search_elem('GBSeq_definition',a)
    
    return [primary_accession, created, updated, protein_seq, dna_seq, taxon, source, description, mol_type]
    
def insert_row(id_, conn):
    start_time = time.time()
    row = 'https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=nucleotide&id={0}&rettype=gb&retmode=xml'
    row = row.format(id_)
    resp = requests.get(row)
    print('getting data {0}'.format(time.time() - start_time))
    tree = ET.ElementTree(ET.fromstring(resp.text))
    
    start_time = time.time()
    pool = Pool(3)
    elements = pool.map(extract_from_xml, [a for a in tree.getroot()])
    pool.close()
    pool.join()
    print('xml parsing {0}'.format(time.time() - start_time))
      
    start_time = time.time()
    for row in elements:
        if row is not None:
            sql = '''INSERT OR REPLACE INTO sequences(PA, created, updated, protein_seq, dna_seq, taxon, source, description, mol_type)
                VALUES("{0}","{1}","{2}","{3}","{4}","{5}", "{6}", "{7}", "{8}")'''
            sql = sql.format(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8])

            cur = conn.cursor()
            cur.execute(sql)
    print('iserting {0}'.format(time.time() - start_time))

if __name__ == "__main__":
    conn = create_connection('genbank.db')

    check_nums = 0

    seq_list = get_sequence_ids_for_taxon(1063)
    print('seq_list got')

    offset = 150
    from_ = 0
    to = offset

    start_time = time.time()

    while from_ < len(seq_list):
            print(to)
            seq_string = ','.join(seq_list[from_:to])
            insert_row(seq_string, conn)
            conn.commit()
            from_ = to
            to += offset
            print('OK')

    conn.commit()    
    conn.close()
    print('all time {0}'.format(time.time() - start_time))