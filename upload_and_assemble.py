'''
This script uploads fastq files from your staging area and uploads them them into Reads objects in
the KBase Workspace / Object Store

After waiting for the upload job to complete, and inspecting the output object,
the launch a spades assembly job

The result is the object id of the spades assembly job
'''
import csv
import os
import sys
from dataclasses import dataclass
from pprint import pprint
from typing import List
import time

from installed_clients.execution_engine2Client import execution_engine2 as EE2

ee2 = EE2(url="https://kbase.us/services/ee2", token=os.environ['KB_AUTH_TOKEN'])
wsid = 71011  # TODO put this somewhere else

upload_job_filenames = {}



@dataclass
class FASTQ_or_SRA_to_READS_input:
    fastq_fwd_staging_file_name: str
    fastq_rev_staging_file_name: str
    name: str
    workspace_name: str  # Example bsadkhin:narrative_1598898899343
    sequencing_tech: str = "Illumina"  # or "PacBio CLR", "PacBio CCS", "IonTorrent", "NanoPore", "Unknown"
    import_type: str = "FASTQ/FASTA"  # or "SRA"
    insert_size_mean: int = None
    insert_size_std_dev: int = None
    interleaved: int = 0  # 0/1 Boolean
    read_orientation_outward: int = 0  # 0/1 Boolean
    single_genome: int = 1  # 0/1 Boolean
    sra_staging_file_name: str = ""  # Relative Path to a file in your staging area


def get_fastq_to_reads_inputs() -> List[FASTQ_or_SRA_to_READS_input]:
    uploader_inputs = []
    with open('inputs.csv') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            upload_input = FASTQ_or_SRA_to_READS_input(
                fastq_fwd_staging_file_name=row['fastq_fwd_staging_file_name'],
                fastq_rev_staging_file_name=row['fastq_rev_staging_file_name'],
                name=row['name'],
                workspace_name="bsadkhin:narrative_1598898899343")
            uploader_inputs.append(upload_input)
    return uploader_inputs


def submit_fastq_to_reads_jobs(uploader_inputs: List[FASTQ_or_SRA_to_READS_input]) -> List[str]:
    job_ids = []
    for uploader_input in uploader_inputs:
        params = {
            'tag': 'release',
            'wsid': wsid,
            'method': 'kb_uploadmethods.import_reads_from_staging',
            'service_ver': 'd42c2a16cc670db70e8a04e0a2b15b69baec7b22',
            'app_id': 'kb_uploadmethods/import_fastq_sra_as_reads_from_staging',
            'source_ws_objects': [],
            'params': [uploader_input.__dict__],
        }

        try:
            job_id = ee2.run_job(params=params)
            job_ids.append(job_id)
            upload_job_filenames[job_id] = uploader_input.name
        except Exception as e:

            print("Failed to submit upload job", e)
            pprint(params)

    print("Submitted job ids", job_ids)
    return job_ids


def submit_assemble_reads_jobs(uploader_job_ids):
    print("About to assemble reads with spades for ", uploader_job_ids)

    uploader_jobs = {}
    for ujid in uploader_job_ids:
        uploader_jobs[ujid] = 0

    completed_jobs = 0

    while (uploader_jobs):
        job_ids = list(uploader_jobs.keys())
        print("Waiting to submit spades jobs for uploads:", job_ids)
        time.sleep(10)
        for job_state in ee2.check_jobs(params={'job_ids':job_ids })['job_states']:

            job_id = job_state['job_id']
            status = job_state['status']
            print("JOB IS",job_id,status)
            if status == 'completed':
                reads_upa = job_state['job_output']['result'][0]['obj_ref']
                del uploader_jobs[job_id]
                submit_spades_job(reads_upa=reads_upa, name=upload_job_filenames[job_id])
            elif status == 'terminated':
                del uploader_jobs[job_id]

            elif status == 'error':
                del uploader_jobs[job_id]



def submit_spades_job(reads_upa, name):
    # TODO turn params into dataclass
    # TODO pass unique contig output name? What happens if names are overwritten?
    # TODO Specify narrative so it shows up in the browser

    print("About to run spades against", reads_upa)
    params = {'app_id': 'kb_SPAdes/run_SPAdes',
              'method': 'kb_SPAdes.run_SPAdes',
              'tag': 'release',
              'service_ver': "c16af2daf290cf629c3c52e5b03d367158dac32f",
              'source_ws_objects': [reads_upa],
              wsid: wsid,

              'params': [{'dna_source': 'standard',
                          'kmer_sizes': [],
                          'min_contig_length': 500,
                          'output_contigset_name': f'{name}.out',
                          'read_libraries': [reads_upa],
                          'skip_error_correction': 0,
                          'workspace_name': 'bsadkhin:narrative_1598898899343'}]
              }
    try:
        job_id = ee2.run_job(params=params)
        print("Submitted spades job", job_id)
    except Exception as e:
        print("Failed to submit spades job", e)



# submit_assemble_reads_jobs(["5f4de4399534ceb23462c318"])
#
#
# # submit_assemble_reads_jobs(["5f4de59c6db5cc7e11bd8fcf"])
#

def begin_upload():
    uploader_inputs = get_fastq_to_reads_inputs()
    uploader_job_ids = submit_fastq_to_reads_jobs(uploader_inputs)
    if len(uploader_job_ids) != len(uploader_inputs):
        print(
            f"Something went wrong. Expected {len(uploader_inputs)} but only got {len(uploader_job_ids)} job ids")
        sys.exit(1)

    assemble_reads = submit_assemble_reads_jobs(uploader_job_ids)

begin_upload()

# if __name__ == '__main__':
#     import sys
#     print("Uploader Input File", "workspace_id")
#     uploader_input_file = sys.argv[1]
#     wsid = sys.argv[2]
