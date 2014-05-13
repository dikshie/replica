#!/usr/bin/env python

import random
import cProfile
import bisect 
import cPickle as pickle
from operator import itemgetter
#tandai ini model 
class CDN(object):

    def __init__(self, catalog):
        self.type = 'CDN'
        self.upload_bandwidth = 0
        self.peer_list = None
        self.content_catalog = catalog
        self.peer_tracking = dict()
        self.peer_tracking_contents = dict()
        self.video_tracking_time_requested = dict()
        self.video_tracking_number_requested = dict()
        self.cdnhitcounter = 0
        self.peerhitcounter = 0
        self.peertraffic = 0
        self.cdntraffic = 0
        self.p_min = 0
        self.p_max = 0
        self.jumlah = 0
        self.cache_entries = dict()
        self.cache_size = dict()
        self.list_replica = []
        self.counter = 0
        self.first_time = {}
        self.mingguan = {}
        self.counter_2 = range(34)


        
    def set_peer_list(self, peer_list):
        self.peer_list = peer_list


    def weighted_sample(items, n):
    total = float(sum(w for w, v in items))
    i = 0

    w, v = items[0]
    while n:
        x = total * (1 - random.random() ** (1.0 / n))
        total -= x
        while x > w:
            x -= w
            i += 1
            w, v = items[i]
        w -= x
        yield v #ini generator!!!!
        n -= 1


    def catatan_weekly(self, t):
    """
    items = [(0.5, "1"),
         (0.3, "2"),
         (0.2, "3")]
    """
    weekly_prob=[ (0.18297472391,1), (0.264039475009,2), (0.12956933302,3), 
                  (0.0642140243698,4), (0.0789835856467,5), (0.0569299452855,6), 
                  (0.00386022624283,7), (0.00362525594978,8), (0.00493437615387,9),
                  (0.0114799771743,10), (0.0058071229566,11), (0.00228256856097,12), 
                  (0.0026518075929,13), (0.0105736631869,14), (0.00966734919942,15), 
                  (0.00238327011514,16), (0.00375952468866,17), (0.00704910879125,18),
                  (0.012352723977,19), (0.00359168876506,20), (0.00312174817898,21),
                  (0.00500151052331,22), (0.00560571984828,23), (0.00802255714813,24),
                  (0.00671343694404,25), (0.00866033365782,26), (0.0124198583465,27),
                  (0.0154409049713,28), (0.00788828840925,29), (0.00902957268974,30),
                  (0.0105065288174,31), (0.0250075526166,32), (0.0123862911618, 33),
                  (0.00288677788594,34), (0.00657916820516,35) ]

    n=36
    hasil=weighted_sample(items,n)
    temp_hasil = []
    for i in hasil:
        temp_hasil.append(i)
    temp_hasil.sort()
    banyak = temp_hasil.count(t)
    proba = t/float(n)
    if proba < 0.25:
        return -1 #tidak peak
    else:
        return 1 #positif peak



    def get_peer_from_id(self, id):
        #numpeer ternyata berurutan: range(100000)
        #lihat di simulator.py
        return self.peer_list[id]
        

    def get_upload_time(self,content_id):
        """
        ambil upload time
        """
        return self.content_catalog[content_id][1]


    def hitung_kmeans(self, x_cari, y_cari):
        """
        hitung k nn neighbors
        """
        if x_cari == 0:
            for i in range(panjang):
                if (x[i] == x_cari):
                    temp_x_3.append(x[i])
                    temp_y_3.append(y[i])
                    temp_z_3.append(z[i])
                if (x[i] == x_cari+1):
                    temp_x_2.append(x[i])
                    temp_y_2.append(y[i])
                    temp_z_2.append(z[i])
    
            #olah data sesudah
            zz = []
            for i in range(len(temp_y_2)):
                #cari nilai y yng sama dng y_cari
                if temp_y_2[i] == y_cari:
                    zz.append(temp_z_2[i])

            if zz: #kalau zz ada isinya
                hasil.append(sum(zz)/float(len(zz)))
            else:
                #kalau zz kosong
                #masuk bisect utk cari nilai terdekat
                nilai_maks = max(temp_y_2)
                nilai_min = min(temp_y_2)
                #sorting!
                temp_y_2, temp_x_2, temp_z_2 = zip(*sorted(zip(temp_y_2, temp_x_2, temp_z_2)))
                #balikan ke list
                temp_y_2 = list(temp_y_2)
                temp_x_2 = list(temp_x_2)
                temp_z_2 = list(temp_z_2)

                indeks = bisect.bisect_left(temp_y_2,y_cari)

                if y_cari > nilai_maks:
                    indeks=indeks-1
                    zz.append(temp_z_2[indeks-1])
                elif y_cari < nilai_min:
                    zz.append(temp_z_2[indeks])
                else:
                    zz.append(temp_z_2[indeks-1])
                    zz.append(temp_z_2[indeks])
                hasil.append(sum(zz)/float(len(zz)))

            #olah data pada saat
            zz = []
            for i in range(len(temp_y_3)):
                if temp_y_3[i] == y_cari:
                    zz.append(temp_z_3[i])

            if zz: #kalau zz ada isinya
                hasil.append(sum(zz)/float(len(zz)))
            else:
                #kalau zz kosong
                #masuk bisect utk cari nilai terdekat
                nilai_maks = max(temp_y_3)
                nilai_min = min(temp_y_3)
                temp_y_3, temp_x_3, temp_z_3 = zip(*sorted(zip(temp_y_3, temp_x_3, temp_z_3)))
                
                #balikan ke list
                temp_y_3 = list(temp_y_3)
                temp_x_3 = list(temp_x_3)
                temp_z_3 = list(temp_z_3)

                indeks = bisect.bisect_left(temp_y_3,y_cari)

                if y_cari > nilai_maks:
                    indeks=indeks-1
                    zz.append(temp_z_3[indeks-1])
                elif y_cari < nilai_min:
                    zz.append(temp_z_3[indeks])
                else:
                    zz.append(temp_z_3[indeks-1])
                    zz.append(temp_z_3[indeks])
                hasil.append(sum(zz)/float(len(zz)))
            rata=sum(hasil)/float(len(hasil))
            if rata < -0.4: #toleransi 
                return -1
            elif rata > -0.5 and rata < 0.5:  #toleransi
                return 0
            else:
                return 1


        elif x_cari > 0 and x_cari < 33:
            for i in range(panjang):
            #sebelum
                if (x[i] == x_cari-1):
                    temp_x_1.append(x[i])
                    temp_y_1.append(y[i])
                    temp_z_1.append(z[i])
        

                if (x[i] == x_cari+1):
                    temp_x_2.append(x[i])
                    temp_y_2.append(y[i])
                    temp_z_2.append(z[i])

                if (x[i] == x_cari):
                    temp_x_3.append(x[i])
                    temp_y_3.append(y[i])
                    temp_z_3.append(z[i])

            #olah data sebelum
            zz = []
            for i in range(len(temp_y_1)):
                #cari nilai y yng sama dng y_cari
                if temp_y_1[i] == y_cari:
                    zz.append(temp_z_1[i])
            if zz: #kalau zz ada isinya
                hasil.append(sum(zz)/float(len(zz)))
            else:
                #kalau zz kosong
                #masuk bisect utk cari nilai terdekat
                nilai_maks = max(temp_y_1)
                nilai_min = min(temp_y_1)
                #sorting!
                temp_y_1, temp_x_1, temp_z_1 = zip(*sorted(zip(temp_y_1, temp_x_1, temp_z_1)))
                #balikan ke list
                temp_y_1 = list(temp_y_1)
                temp_x_1 = list(temp_x_1)
                temp_z_1 = list(temp_z_1)

                indeks = bisect.bisect_left(temp_y_1,y_cari)

                if y_cari > nilai_maks:
                    indeks=indeks-1
                    zz.append(temp_z_1[indeks-1])
                elif y_cari < nilai_min:
                    zz.append(temp_z_1[indeks])
                else:
                    zz.append(temp_z_1[indeks-1])
                    zz.append(temp_z_1[indeks])
                hasil.append(sum(zz)/float(len(zz)))

            #olah data sesudah
            zz = []
            for i in range(len(temp_y_2)):
                #cari nilai y yng sama dng y_cari
                if temp_y_2[i] == y_cari:
                    zz.append(temp_z_2[i])

            if zz: #kalau zz ada isinya
                hasil.append(sum(zz)/float(len(zz)))
            else:
                #kalau zz kosong
                #masuk bisect utk cari nilai terdekat
                nilai_maks = max(temp_y_2)
                nilai_min = min(temp_y_2)
                #sorting!
                temp_y_2, temp_x_2, temp_z_2 = zip(*sorted(zip(temp_y_2, temp_x_2, temp_z_2)))
                #balikan ke list
                temp_y_2 = list(temp_y_2)
                temp_x_2 = list(temp_x_2)
                temp_z_2 = list(temp_z_2)

                indeks = bisect.bisect_left(temp_y_2,y_cari)

                if y_cari > nilai_maks:
                    indeks=indeks-1
                    zz.append(temp_z_2[indeks-1])
                elif y_cari < nilai_min:
                    zz.append(temp_z_2[indeks])
                else:
                    zz.append(temp_z_2[indeks-1])
                    zz.append(temp_z_2[indeks])
                hasil.append(sum(zz)/float(len(zz)))

            #olah data pada saat
            zz = []
            for i in range(len(temp_y_3)):
                if temp_y_3[i] == y_cari:
                    zz.append(temp_z_3[i])

            if zz: #kalau zz ada isinya
                hasil.append(sum(zz)/float(len(zz)))
            else:
                #kalau zz kosong
                #masuk bisect
                nilai_maks = max(temp_y_3)
                nilai_min = min(temp_y_3)
                temp_y_3, temp_x_3, temp_z_3 = zip(*sorted(zip(temp_y_3, temp_x_3, temp_z_3)))
                #balikan ke list
    
                temp_y_3 = list(temp_y_3)
                temp_x_3 = list(temp_x_3)
                temp_z_3 = list(temp_z_3)

                indeks = bisect.bisect_left(temp_y_3,y_cari)

                if y_cari > nilai_maks:
                    indeks=indeks-1
                    zz.append(temp_z_3[indeks-1])
                elif y_cari < nilai_min:
                    zz.append(temp_z_3[indeks])
                else:
                    zz.append(temp_z_3[indeks-1])
                    zz.append(temp_z_3[indeks])
                hasil.append(sum(zz)/float(len(zz)))

            rata=sum(hasil)/float(len(hasil))
            if rata < -0.4: #toleransi 
                return -1
            elif rata > -0.5 and rata < 0.5:  #toleransi
                return 0
            else:
                return 1
        else: #utk x_cari > 33
            return 1



    def estimasi_vr(self, t_cur):
        """
        estimasi t before/at/after berdasarkan minggu dan view rate
        membutuhkan data view_rate dan minggu

        """
        #dapatkan minggu video
        #selisih = t_cur - t_upload
        upload_time = this_content[1]
        content_id = this_content[0]
        waktu_terakhir_akses=self.get_video_last_time_requested(content_id)
        selisih = waktu_terakhir_akses - upload_time

        minggu,second = divmod(selisih, 7*24*60*60)

        #kalau minggu 0
        #view rate yng dipakai 
        #selisih view count dng pertama kali upload
        if minggu == 0:
            view_rate = self.get_number_requested_video(content_id) - 1

        else:
            #kalau minggu > 0
            #view rate yng dipakai view rate pada minggu tsb
            #view count pada saat ini - view count terakhir minggu lalu
            view_count_saat_ini = self.get_number_requested_video(content_id)-1
            view_count_terakhir_minggu_lalu = self.mingguan[content_id][minggu-1] 
            
            view_rate = view_count_saat_ini - view_count_terakhir_minggu_lalu

        hasil=self.hitung_kmeans(minggu,view_rate)
        return hasil


    def get_number_requested_video(self, content_id):
        """
        ambil jumlah video yng telah di request
        """
        #print self.video_tracking_number_requested
        return self.video_tracking_number_requested[content_id]



    def get_video_last_time_requested(self, content_id):
        """
        ambil waktu terakhir video direquest
        """
        return self.video_tracking_time_requested[content_id]['last']


    def get_first_time_requested(self, content_id):
        """
        ambil jumlah pertama kali video diakses
        """
        if self.video_tracking_number_requested[content_id] < 2:
            return 1  #positif pertama kali akses
        else:
            return -1 #negatif, sudah ada yng akses



    def hitung_p_min(self, time_cur):
        """
        hitung p_min utk semua video yng ada
        """
        temp1 = []
        #hitung utk semua video yng beredar di sistem
        con = self.peer_tracking_contents.keys()
        for i in con:
            n_ir = self.get_number_requested_video(i)
            t_ir = self.get_video_last_time_requested(i)
            a_i = self.content_catalog[i][1]
            kanan = (1.0)/abs(time_cur - t_ir)
            kiri = (n_ir)/abs(t_ir - a_i)
            temp1.append(min(kiri,kanan))
        self.p_min = min(temp1)
        temp1 = []
        return self.p_min 


    def hitung_p_max(self, time_cur):
        """
        hitung p_max utk semua video yng ada
        """
        temp2=[]
        #hitung utk semua video yng beredar di sistem
        con = self.peer_tracking_contents.keys()
        for i in con: 
            n_ir = self.get_number_requested_video(i)
            t_ir = self.get_video_last_time_requested(i)
            a_i = self.content_catalog[i][1]
            kanan = (1.0)/abs(time_cur - t_ir)
            kiri = (n_ir)/abs(t_ir - a_i)
            temp2.append(min(kiri,kanan))
        self.p_max = max(temp2)
        temp2 = []
        return self.p_max 


    def get_replica(self, content_id):
        """
        dapatkan jumlah replica utk sebuah video id
        """
        self.jumlah=0
        if not self.peer_tracking_contents.has_key(content_id):
            self.jumlah=0
        else:
            self.jumlah = len(self.peer_tracking_contents[content_id].keys())

        return self.jumlah


    def get_log(self):
        """
        log 
        """
        temp3 = {}
        #hitung replica utk semua video yng beredar di sistem
        con = self.peer_tracking_contents.keys()
        for i in con:
            temp3[i]=self.get_replica(i)
        r_max = max(temp3.values())
        r_min = min(temp3.values())
        if r_min == 0:
            r_min = 1
        optimum = (2*r_max*r_min)/(r_max+r_min)
        return (temp3, optimum)


    def get_content(self, peer_id, content_id, time_cur):
        """
        reply dengan content atau redirect
        """
        this_content=self.content_catalog[content_id]
        tracked_peer = self.get_peer_tracking(content_id)

        #rekam waktu video direquested
        if not self.video_tracking_time_requested.has_key(content_id):
            self.video_tracking_time_requested[content_id]={}
            self.video_tracking_time_requested[content_id]['new']=time_cur
        
        self.video_tracking_time_requested[content_id]['last']=self.video_tracking_time_requested[content_id]['new']
        self.video_tracking_time_requested[content_id]['new']=time_cur



        #results_p[p] = results_p.get(p, 0) + 1
        #rekam jumlah akses video
        self.video_tracking_number_requested[content_id] = self.video_tracking_number_requested.get(content_id,0)+1
        #print self.video_tracking_number_requested
        
        #catatan view count per minggu
        #ambil waktu pertama kali video di upload
        upload_time=this_content[1]

        #hitung selisih antara time_cur dng waktu upload video
        selisih = abs(time_cur - upload_time)

        jumlah_request = self.video_tracking_number_requested[content_id]
        if not self.mingguan.has_key(content_id):
            self.mingguan[content_id]={}
            #self.mingguan[content_id]['pertamaupload']=upload_time

        #self.mingguan[content_id]['timerequest']=time_cur
        #self.mingguan[content_id]['jumlah']=jumlah_request

        minggu,second = divmod(selisih, 7*24*60*60)
        self.mingguan[content_id][minggu]=jumlah_request
        

        if tracked_peer:
            #print 'tracked_peer', tracked_peer , 'content_id', content_id
            #print 'p'
            self.peerhitcounter += 1
            return [None, tracked_peer]
            
        #jika content tidak ada di peer (None) maka kembalikan langsung dari CDN.
        else:
            #print 'cdnhit', self.cdnhitcounter
            self.cdnhitcounter += 1

            #check cache_size
            jumlah = sum(self.cache_size.values())
            ukuran_video_baru = this_content[2]
            #print jumlah+ukuran_video_baru

            if (jumlah+ukuran_video_baru) < 10000: #10000MB atau 10GB maka langsung cache
                #cache content
                self.cache_entries[content_id]=this_content
                self.cache_size[content_id]=this_content[2]
                return [this_content, None]
            else:
                #hitung popularity video yng baru
                #print jumlah+ukuran_video_baru
                n_ir = self.get_number_requested_video(content_id)
                t_ir = self.get_video_last_time_requested(content_id)
                a_i = self.content_catalog[content_id][1]
                #print n_ir, time_cur, t_ir, a_i
                if (time_cur - t_ir) == 0:
                    kanan = 0.0
                else:
                    kanan = (1.0)/abs(time_cur - t_ir)

                kiri = (n_ir)/abs(t_ir - a_i)
                P_i_video_baru = min(kiri,kanan)

                #hitung popularity video yng ada didalam cache
                con = self.cache_entries.keys()
                temp={}
                for i in con:  #utk tiap video id didalam cache
                    #hitung popularity
                    n_ir = self.get_number_requested_video(i)
                    t_ir = self.get_video_last_time_requested(i)
                    a_i = self.content_catalog[i][1]
                    if (time_cur - t_ir) == 0:
                        kanan = 0.0
                    else:
                        kanan = (1.0)/abs(time_cur - t_ir)
                    kiri = (n_ir)/abs(t_ir - a_i)
                    P_i = min(kiri,kanan)
                    temp[i]=P_i
                #sort dict berdasarkan values!
                #hasilnya berupa list of tuple:  [(),()]
                list_sorted_P_i = sorted(temp.items(), key=itemgetter(1))
                P_i_min_video_dicache = list_sorted_P_i[0][1]

                if P_i_min_video_dicache < P_i_video_baru: #video dalam cache dihapus
                    jumlah=sum(self.cache_size.values())
                    #iteratively remove video in cache!
                    while (jumlah+ukuran_video_baru) >= 10000: #lebih dari 10GB
                        tup = list_sorted_P_i.pop(0)
                        video_id_dng_p_min = tup[0]
                        video_size_dng_p_min = tup[1]

                        del self.cache_size[video_id_dng_p_min]
                        del self.cache_entries[video_id_dng_u_min]
                        jumlah=sum(self.cache_size.values())
                    self.cache_entries[content_id]=this_content
                    self.cache_size[content_id]=this_content[2]
                    return [this_content, None]
                else:
                    #popularity min video didalam cache > popularity video baru
                    #maka video baru tidak perlu dicache
                    #dan video dalam cache diperlu dihapus
                    return [this_content, None]
                    #pass
            #return [this_content, None]
    

    def receive_report_from_peer(self, peer_id, content_id, type, time_cur, yng_request, log_replica=None):
        """
        report to cdn (model baru)
        type = CACHE, REMOVE_CACHE, UPLOADING, IDLE
        
        peer_tracking adalah dictionary dengan peer_id sebagai key, dan tiap value adalah dictionary dengan key peer_id, status, dan cache_content
        """

        #init dict
        if not self.peer_tracking_contents.has_key(content_id):
            self.peer_tracking_contents[content_id] = dict()

        #if not self.log_replica.has_key(content_id):
        #    self.log_replica[time_cur]= dict()
        #    self.log_replica[time_cur] = {'video-id': 'NONE', 'self-id':'NONE',  't_di_cache':'NONE',  't_di_remove':'NONE', 't_di_access':[]}
            #self.log_replica[content_id] = dict()
            #self.log_replica[content_id] = {'self-id':'NONE',  't_di_cache':'NONE',  't_di_remove':'NONE', 't_di_access':[]}

        #if self.peer_list[peer_id].upload_state:
            #print 'PIDD', self.peer_list[peer_id].upload_state
        #if type == 'UPLOADING':
        #    self.log_replica[time_cur]['video-id'] = content_id
        #    self.log_replica[time_cur]['self-id'] = yng_request
        #    self.log_replica[time_cur]['t_di_access'].append(  (time_cur, peer_id)  ) 

        if type == 'CACHE':
            self.peer_tracking_contents[content_id][peer_id]=peer_id
            #print self.peer_tracking_contents
            #self.log_replica[time_cur]['video-id'] = content_id
            #self.log_replica[time_cur]['self-id']=peer_id
            #self.log_replica[time_cur]['t_di_cache']=time_cur
            

        if type == 'REMOVE_CACHE':
            del self.peer_tracking_contents[content_id][peer_id]            
            self.list_replica.append(log_replica)
            
            if (self.counter%64)==(64-1):
                filename = 'replica-log-' + str(self.counter) + '.pickle'
                with open(filename,'wb') as fd:
                    pickle.dump(self.list_replica,fd)
                fd.close()
                self.list_replica=[]
            self.counter+=1  


    def store_last_state(self):

        print 'data store last'
        #sisa dari 'REMOVE_CACHE', yng masih didalam memori belum ditulis.
        filename = 'replica-log-' + str(self.counter) + '.pickle'
        with open(filename,'wb') as fd:
            pickle.dump(self.list_replica,fd)
        fd.close()


        #log last states
        list_replica_local=[]
        for i in self.peer_list:
            for k,v in i.log_replica.iteritems():
                list_replica_local.append(v)

            if len(list_replica_local)>50000:
                filename = 'replica-log-akhir-' + str(self.counter) + '.pickle'
                with open(filename,'wb') as fd:
                    pickle.dump(list_replica_local,fd)
                fd.close()
                list_replica_local=[]
                self.counter+=1

        filename = 'replica-log-akhir-' + str(self.counter) + '.pickle'
        with open(filename,'wb') as fd:
            pickle.dump(list_replica_local,fd)
        fd.close()
        list_replica_local=[]
        self.counter+=1


        
    def get_peer_tracking(self, content_id):
        """
        CDN harus punya catatan peer mana saja yng sudah punya content
        content request yng ada di peer di-redirect ke peer yng sudah punya content
        """
        #if not self.peer_tracking_contents.has_key(content_id):
        #    return None
        #else:
        #    if self.peer_tracking_contents[content_id]:
        #        #print 'True', self.peer_tracking_contents
        #        daftar_peer_idle = []
        #        for key, status in self.peer_tracking_contents[content_id].iteritems():
        #            if status == 'IDLE':
        #                daftar_peer_idle.append(key)
        #        #print content_id, daftar_peer_idle
        #        if daftar_peer_idle:
        #            k = random.choice(daftar_peer_idle)
        #            #print k
        #            return self.get_peer_from_id(k)
        #    else:
        #        print 'False'
        #        return None

        daftar_peer_idle = []
        
        if content_id in self.peer_tracking_contents:

            #kunci=self.peer_tracking_contents[content_id].keys()
            #for p in kunci:
            #    print '--->', self.peer_list[p].upload_state
            daftar_peer_idle = [ pid for pid in self.peer_tracking_contents[content_id].keys() if self.peer_list[pid].upload_state == 'IDLE']
            if daftar_peer_idle:
                #print daftar_peer_idle
                k=random.choice(daftar_peer_idle)
                return self.get_peer_from_id(k)
            else:
                return None

        else:
            return None
        # for k,v in self.peer_tracking.iteritems():
        #     if content_id in v['cache_content'] and v['status']=='IDLE':
        #         daftar_peer_idle.append(k)
            
        #     #kalau ada hasil baik banyak element atau cuma satu element:
        # if daftar_peer_idle:  
        #     k = random.choice(daftar_peer_idle)
        #     return self.get_peer_from_id(k)
        # else:
        #     return None
                    
                    
                
