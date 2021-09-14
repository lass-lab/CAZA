#!/usr/bin/python

import sys
import re

if __name__ == "__main__" :
    fname = sys.argv[1]
    
    infile = open(fname, 'r')
    gc_cnt = 0;

    inval_cnt = {}
    lt_cnt = {}

    while True:
        
        line = infile.readline()

        if not line :
            break


        if line[0] == 'T':
            tmp = re.split(' : |\n', line)
            data_copied = tmp[1] 
            print("copied-data " + str(data_copied))
        
        elif line[10] == 'T':
            cur_gc = gc_cnt - 1
            print("GC-COUNT " + str(cur_gc))
            print("INVAL-COUNT")
            for keys, values in inval_cnt.items() :
                print(str(keys) + " " + str(values))

            print("LIFETIME-COUNT")
            for keys, values in lt_cnt.items() :
                print(str(keys) + " " + str(values))
            
            tmp = re.split(' : |\n|\*', line)
            print("TOTAL-COPIED-WHILE-GC " + str(tmp[11]))
            print("\n") 
        elif line[0] == '*' :
            gc_cnt += 1
            
            if gc_cnt is not 1 :
                cur_gc = gc_cnt - 1
                print("GC-COUNT " + str(cur_gc))
                print("INVAL-COUNT")
                for keys, values in inval_cnt.items() :
                    print(str(keys) + " " + str(values))

                print("LIFETIME-COUNT")
                for keys, values in lt_cnt.items() :
                    print(str(keys) + " " + str(values))
                print("\n")
                inval_cnt.clear()
                lt_cnt.clear()

        elif line[0] == 'z' :
            tmp = re.split(' : |\n', line)
            lt = tmp[1]
            if lt not in lt_cnt :
                lt_cnt[lt] = 1
            else :
                lt_cnt[lt] = lt_cnt[lt] + 1

        elif line[0] == 'i' :
            tmp = re.split(' : |\n', line)
            ratio = tmp[1]
            if ratio not in inval_cnt :
                inval_cnt[ratio] = 1
            else :
                inval_cnt[ratio] = inval_cnt[ratio] + 1



