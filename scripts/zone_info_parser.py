#!/usr/bin/python

import sys
import re
import csv

compaction_input_set = {}
global_zone_info = []

fno_per_zone = [] # key : zone start, value : list of valid sst list and invalid sst list 
gc_cnt = 0
elpased_time = 0
def PrintZoneSSTStatusAsCSV(outf_writer) :
    num_zone = len(fno_per_zone)

    max_inval_sst = 0
    max_val_sst = 0
    
    zone_idx = 0
    list_idx = zone_idx + 1
    # now fno_per_zone is list correct it
    while zone_idx < num_zone :
       
        sst_lists = fno_per_zone[list_idx]

        num_inval_sst = len(sst_lists[0])
        num_val_sst = len(sst_lists[1])

        if max_inval_sst < num_inval_sst :
            max_inval_sst = num_inval_sst

        if max_val_sst < num_val_sst :
            max_val_sst = num_val_sst

        zone_idx += 2
        list_idx = zone_idx + 1  

    zone_idx = 0
    list_idx = zone_idx + 1

    while zone_idx < num_zone :
                
        sst_lists = fno_per_zone[list_idx]

        num_inval_sst = len(sst_lists[0])
        num_val_sst = len(sst_lists[1])

        num_inval_padd = 0
        num_val_padd = 0

        if max_inval_sst > num_inval_sst :
            num_inval_padd = max_inval_sst - num_inval_sst

        if max_val_sst > num_val_sst :
            num_val_padd = max_val_sst - num_val_sst

        while num_inval_padd > 0 :
            sst_lists[0].append("-")
            num_inval_padd -= 1

        while num_val_padd > 0 :
            sst_lists[1].append("-")
            num_val_padd -= 1
 
        zone_idx += 2
        list_idx = zone_idx + 1  

    zone_idx = 0
    list_idx = zone_idx + 1

    tmp = []
    tmp.append((len(fno_per_zone)/2))
    tmp.append(max_inval_sst);
    tmp.append(max_val_sst);
    outf_writer.writerow(tmp)

    col = []
    col.append("zone_start")

    ext_cnt = 0
    while ext_cnt < max_inval_sst :
        col.append("inval_sst"+str(ext_cnt))
        ext_cnt += 1

    ext_cnt = 0
    while ext_cnt < max_val_sst :
        col.append("val_sst"+str(ext_cnt))
        ext_cnt += 1

    outf_writer.writerow(col)

    while zone_idx < num_zone :
   
        cur_row = []
        cur_row.append(fno_per_zone[zone_idx])

        for inval_fno in fno_per_zone[list_idx][0] :
            cur_row.append(inval_fno)
        
        for val_fno in fno_per_zone[list_idx][1] :
            cur_row.append(val_fno)
 
        outf_writer.writerow(cur_row)

        zone_idx += 2
        list_idx = zone_idx + 1  

    fno_per_zone.clear()

def CountOverlappedSST(sst_dict) :
    
    files = sst_dict.keys()
    dummy_files = files
    
    num_files = len(files)
    cnt = 0

    if num_files == 0 :
        return 0

    if num_files == 1:
        return 1

    for dst_fno in files :

        for src_fno in files :

            if dst_fno == src_fno :
                continue

            else :
                for fno_list in compaction_input_set.values() :
                    
                    if (src_fno in fno_list) and (dst_fno in fno_list) :
                        cnt += 1
    return cnt

def GetCompactionSet(cin_fname) :
 
    cinf = open(cin_fname, 'r')
    job_id = 0
    
    while True:
        line = cinf.readline()
        
        if not line :
            break

        # job_id
        if line[0] == 'j':
            input_list = []
            num_file_info = cinf.readline()
            num_files = int(re.split(' : |\n', num_file_info)[1])
            
            while num_files > 0 :
                fileno = int(cinf.readline())
                input_list.append(fileno)
                num_files -= 1

            compaction_input_set[job_id] = input_list
            job_id += 1

def ConvertCSVForm(copied_data, outf_writer) :

    col = ["GC "+str(gc_cnt), "COPIED "+str(copied_data), "TimeStamp "+str(elapsed_time)]
    outf_writer.writerow(col)

    max_val_sst = 0
    max_val_log = 0 
    
    max_inval_sst = 0
    max_inval_log = 0 

    for zone_info in global_zone_info :
        inval_sst_num = zone_info[8]
        inval_log_num = zone_info[10]

        val_sst_num = zone_info[11]
        val_log_num = zone_info[13]
 
        if inval_sst_num > max_inval_sst :
            max_inval_sst = inval_sst_num

        if inval_log_num > max_inval_log :
            max_inval_log = inval_log_num
       
        if val_sst_num > max_val_sst :
            max_val_sst = val_sst_num

        if val_log_num > max_val_log :
            max_val_log = val_log_num

    for zone_info in global_zone_info :
        inval_sst_num = zone_info[8]
        inval_log_num = zone_info[10]

        val_sst_num = zone_info[11]
        val_log_num = zone_info[13]

        if inval_sst_num < max_inval_sst :
            diff = max_inval_sst - inval_sst_num

            while diff > 0 :
                zone_info[14].append("-")
                diff -= 1
        
        if inval_log_num < max_inval_log :
            diff = max_inval_log - inval_log_num

            while diff > 0 :
                zone_info[15].append("-")
                diff -= 1

        if val_sst_num < max_val_sst :
            diff = max_val_sst - val_sst_num

            while diff > 0 :
                zone_info[16].append("-")
                diff -= 1

        if inval_log_num < max_inval_log :
            diff = max_inval_log - inval_log_num

            while diff > 0 :
                zone_info[17].append("-")
                diff -= 1
   
    col = []

    col.append("start")
    col.append("wp")
    col.append("used_capacity")
    col.append("max_capcaity")
    col.append("invalid_bytes")
    col.append("valid_bytes")
    col.append("remain_capacity")
    col.append("LifeTimeHint")
    col.append("invalid SST Count")
    col.append("invalid Compacted SST Count")
    col.append("invalid LOG Count")
    col.append("valid SST Count")
    col.append("valid Compacted SST Count")
    col.append("valid LOG Count")

    ext_cnt = 0
    while ext_cnt < max_inval_sst :
        col.append("inval_sst_ext" + str(ext_cnt))
        ext_cnt += 1

    ext_cnt = 0   
    while ext_cnt < max_inval_log :
        col.append("inval_log_ext" + str(ext_cnt))
        ext_cnt += 1

    ext_cnt = 0
    while ext_cnt < max_val_sst :
        col.append("val_sst_ext" + str(ext_cnt))
        ext_cnt += 1

    ext_cnt = 0   
    while ext_cnt < max_val_log :
        col.append("val_log_ext" + str(ext_cnt))
        ext_cnt += 1
    
    col.append("invalid dbtmp Count")
    col.append("invalid mani Count")
    col.append("invalid dbtmp total length")
    col.append("invalid main  total length")
 
    col.append("valid dbtmp Count")
    col.append("valid mani Count")
    col.append("valid dbtmp total length")
    col.append("valid main  total length")  
    
    outf_writer.writerow(col)
    
    for zone_info in global_zone_info :
        
        cur_row = []
        cur_row.append(zone_info[0])
        cur_row.append(zone_info[1])
        cur_row.append(zone_info[2])
        cur_row.append(zone_info[3])
        cur_row.append(zone_info[4])
        cur_row.append(zone_info[5])
        cur_row.append(zone_info[6])
        cur_row.append(zone_info[7])
        cur_row.append(zone_info[8])
        cur_row.append(zone_info[9])
        cur_row.append(zone_info[10])
        cur_row.append(zone_info[11])
        cur_row.append(zone_info[12])
        cur_row.append(zone_info[13])

        for inval_sst in zone_info[14] :
            cur_row.append(inval_sst)

        for inval_log in zone_info[15] :
            cur_row.append(inval_log)

        for val_sst in zone_info[16] :
            cur_row.append(val_sst)
            
        for val_log in zone_info[17] :
            cur_row.append(val_log)

        cur_row.append(zone_info[18])
        cur_row.append(zone_info[19])
        cur_row.append(zone_info[20])
        cur_row.append(zone_info[21])
        cur_row.append(zone_info[22])
        cur_row.append(zone_info[23])
        cur_row.append(zone_info[24])
        cur_row.append(zone_info[25])

        outf_writer.writerow(cur_row)
    global_zone_info.clear()


if __name__ == "__main__" :
    zinfname = sys.argv[1] # raw data set for zone information
    cinfname = sys.argv[2] # raw data set for compaction information
    ofname = sys.argv[3]
    ofname_2 = sys.argv[3][:len(sys.argv[3])-3]+"_files.csv"

    outf = open(ofname, "w", newline='')
    outf_2 = open(ofname_2, "w", newline='')

    outf_writer = csv.writer(outf)
    outf_writer_2 = csv.writer(outf_2)

    GetCompactionSet(cinfname)

    zinf = open(zinfname, 'r')
    verbose_info_cnt = 0
    
    #RocksDB and Zone informations
    while verbose_info_cnt < 20 :
        line = zinf.readline()
        verbose_info_cnt += 1

    inval_cnt = {}
    lt_cnt = {}

    while True:
        line = zinf.readline()

        if not line :
            break

        # GC count
        if line[0] == 'G':
            tmp = re.split(' : |"T"|\n', line)
            elapsed_time = int(tmp[2])
            gc_cnt = int(tmp[1][:len(tmp[1])-4])
        elif line[0] == 'Z':
            inval_ext_cnt = 0;
            val_ext_cnt = 0;
            cur_zone_info = []

            #Zone default information
            start_info = zinf.readline()
            wp_info = zinf.readline()
            used_info = zinf.readline()
            max_info = zinf.readline()
            inval_byte_info = zinf.readline()
            val_byte_info = zinf.readline()
            remain_info = zinf.readline()
            lt_info = zinf.readline()
            
            inval_ext_info = zinf.readline()
            val_ext_info = zinf.readline()

            start = re.split(' : |\n', start_info)
            wp = re.split(' : |\n', wp_info)
            used = re.split(' : |\n', used_info)
            max_bytes = re.split(' : |\n', max_info)
            inval_bytes = re.split(' : |\n', inval_byte_info)
            val_bytes = re.split(' : |\n', val_byte_info)
            remain = re.split(' : |\n', remain_info)
            lt = re.split(' : |\n', lt_info)

            val_cnt = re.split(' : |\n', val_ext_info)
            inval_cnt = re.split(' : |\n', inval_ext_info)
           
            val_cnt = int(val_cnt[1])
            inval_cnt = int(inval_cnt[1])

            cur_zone_info.append(int(start[1]))
            cur_zone_info.append(int(wp[1]))
            cur_zone_info.append(int(used[1]))
            cur_zone_info.append(int(max_bytes[1]))
            cur_zone_info.append(int(inval_bytes[1]))
            cur_zone_info.append(int(val_bytes[1]))
            cur_zone_info.append(str(remain[1]))
            cur_zone_info.append(int(lt[1]))
            
            dummy = zinf.readline() #Valid Extent Information
            
            inval_sst_dict = {}
            inval_log_length = []
            inval_dbtmp_length = []
            inval_mani_length = []
            
            inval_total_dbtmp_length = 0
            inval_total_mani_length = 0

            val_sst_dict = {}
            val_log_length = []
            val_dbtmp_length = []
            val_mani_length = []

            val_total_dbtmp_length = 0
            val_total_mani_length = 0

            while inval_cnt > 0 :
                                
                ext_fname_info = zinf.readline()
                ext_fname = re.split(' : |\n', ext_fname_info)
                ext_fname = str(ext_fname[1])

                if ext_fname[-3:] == "sst" :
                    sst_num = int(ext_fname[-10:-4])
                    length_info = zinf.readline()
                    length = re.split(' : |\n', length_info)
                    length = int(length[1])

                    inval_sst_dict[sst_num] = length

                elif ext_fname[-3:] == "log" :
                    length_info = zinf.readline()
                    length = re.split(' : |\n', length_info)
                    length = int(length[1])

                    inval_log_length.append(length)

                elif ext_fname[-5:] == "dbtmp" :
                    length_info = zinf.readline()
                    length = re.split(' : |\n', length_info)
                    length = int(length[1])

                    inval_total_dbtmp_length += length
                    inval_dbtmp_length.append(length)

                else :
                    #manifest
                    length_info = zinf.readline()
                    length = re.split(' : |\n', length_info)
                    length = int(length[1])
 
                    inval_total_mani_length += length
                    inval_mani_length.append(length)
                
                inval_cnt -=1

            inval_overlapped = CountOverlappedSST(inval_sst_dict)

            cur_zone_info.append(len(inval_sst_dict.items()))
            cur_zone_info.append(inval_overlapped)
            cur_zone_info.append(len(inval_log_length))

            dummy = zinf.readline() #Valid Extent Information

            while val_cnt > 0 :

                ext_fname_info = zinf.readline()
                ext_fname = re.split(' : |\n', ext_fname_info)
                ext_fname = str(ext_fname[1])

                if ext_fname[-3:] == "sst" :
                    sst_num = int(ext_fname[-10:-4])
                    length_info = zinf.readline()
                    length = re.split(' : |\n', length_info)
                    length = int(length[1])
                   
                    val_sst_dict[sst_num] = length

                elif ext_fname[-3:] == "log" :
                    length_info = zinf.readline()
                    length = re.split(' : |\n', length_info)
                    length = int(length[1])
                    
                    val_log_length.append(length)

                elif ext_fname[-5:] == "dbtmp" :
                    length_info = zinf.readline()
                    length = re.split(' : |\n', length_info)
                    length = int(length[1])
 
                    val_total_dbtmp_length += length
                    val_dbtmp_length.append(length)

                else :
                    #manifest
                    length_info = zinf.readline()
                    length = re.split(' : |\n', length_info)
                    length = int(length[1])
                    
                    val_total_mani_length += length
                    val_mani_length.append(length)
                
                val_cnt -=1
            val_overlapped = CountOverlappedSST(val_sst_dict)
            
            cur_zone_info.append(len(val_sst_dict.items()))
            cur_zone_info.append(val_overlapped)
            cur_zone_info.append(len(val_log_length))

            cur_zone_info.append(list(inval_sst_dict.values()))
            cur_zone_info.append(inval_log_length)
            cur_zone_info.append(list(val_sst_dict.values()))
            cur_zone_info.append(val_log_length)

            cur_zone_info.append(len(inval_dbtmp_length))
            cur_zone_info.append(len(inval_mani_length))
            cur_zone_info.append(inval_total_dbtmp_length)
            cur_zone_info.append(inval_total_mani_length)

            cur_zone_info.append(len(val_dbtmp_length))
            cur_zone_info.append(len(val_mani_length))
            cur_zone_info.append(val_total_dbtmp_length)
            cur_zone_info.append(val_total_mani_length)

            global_zone_info.append(cur_zone_info)

            valsst_list = list(val_sst_dict.keys())
            invalsst_list = list(inval_sst_dict.keys())

            inval_val_sst = []
            inval_val_sst.append(invalsst_list)
            inval_val_sst.append(valsst_list)

            if int(remain[1]) == 0 :
                fno_per_zone.append(start[1])
                fno_per_zone.append(inval_val_sst)

        elif line[0] == 'T':
            tmp = re.split(' : |\n', line)
            copied_data = int(tmp[1])

            ConvertCSVForm(copied_data, outf_writer)
            PrintZoneSSTStatusAsCSV(outf_writer_2)
