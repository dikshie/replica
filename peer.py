#!/usr/bin/env python

import random 
import event
import math
#import cProfile
from collections import OrderedDict
from operator import itemgetter
#tandai ini model

# class properties:  
# cache: cache entry (file_id, size, start, stop, download count), expire, request, upload

class Peer(object):
    
    #method __init__
    def __init__(self, id, cdn, size, up_bw, dn_bw):
        self.cache_entries=dict() #// file_id, size/len, expire
        self.size=size
        self.cdn = cdn
        self.id = id
        self.up_bw = up_bw
        self.dn_bw = dn_bw
        self.type = 'Peer'
        self.upload_bandwidth = 0
        self.upload_state = 'IDLE'
        self.cache_size=dict()
        self.log_replica = dict()
             
    def __repr__(self):
        return self.id
        
    def __str__(self):
        return str(self.id)
    
    def request_to_cdn(self, content_id, time_cur):
        """
        request ke CDN
        CDN reply dengan content atau redirect ke peer lain
        buat event untuk cache content dari CDN jika CDN reply dengan content
        buat event untuk request_to_peer jika CDN redirect ke peer lain
        """
        
                
        # cek cache entries dulu utk mencegah kasus peer yng sama me-request content id yng sama
        #
        # jika cache_entries[content_id] sudah ada return []
        if self.cache_entries.has_key(content_id):
            return [],[]
        
        other_reply = self.cdn.get_content(self.id, content_id, time_cur)
        if other_reply[0]:
            content = other_reply[0]
       
                        
            #pada prinsipnya cache_event ini adalah mendownload kemudian menyimpan 
            #cache_content event, download duration, actor, actor action, action parameters
            # tandai bahwa cache entry akan di-isi content sebenarnya
            #self.cache_entries[content_id] = 1
            #contoh: {0: [0, 5.0775931306168065, 499.0, 3600.0, week_peak]}
            durasi = content[2]*8/(self.dn_bw/1000.0)
            cache_event_cdn = event.Event(event.CACHE_CONTENT, time_cur+durasi, self, self.cache_content_cdn, [content_id, content, time_cur+durasi])

            return [cache_event_cdn],[]
            
        else:
            #event request to peer
            request_to_peer_event = event.Event(event.REQUEST, time_cur, self, self.request_to_peer, [content_id, other_reply[1], time_cur])
            return [request_to_peer_event],[]
            
        
    def request_to_peer(self, content_id, other, time_cur):
        """
        request ke peer lain (other)
        """

        yng_request = self.id
        #ambil content ke peer, hasilnya content 
        content = other.get_content(content_id,time_cur,yng_request) 
        #peer_origin = other
        
        #ekstrak nilai panjang file
        #print content
        #contoh: {0: [0, 5.0775931306168065, 499.0, 3600.0, week]}
        durasi_up = (content[2]*8)/(self.up_bw/1000.0)

        
        #event utk upload done
        upload_done_event = event.Event(event.UPLOAD_DONE, time_cur+durasi_up, other, other.upload_done, [content_id, other.id, 'BUSY', time_cur, yng_request])
        #print upload_done_event
        #print 'hhh', content
        #new_remove_content_event, old_remove_content_event = other.change_content_expiry_time(content, time_cur)
              
        #hitung durasi
        #time_duration = (content[1]*8)/(other.up_bw/1000.0)
        durasi_down = (content[2]*8)/(self.dn_bw/1000.0)
        # tandai bahwa cache entry akan di-isi content sebenarnya
        #self.cache_entries[content_id] = 1
        
        # bikin cache_content event:
        cache_event = event.Event(event.CACHE_CONTENT, time_cur+durasi_down, self, self.cache_content, [content_id, content, time_cur+durasi_down])
        
        #kembalikan event
        #return [cache_event, upload_done_event, new_remove_content_event] , [old_remove_content_event]
        return [cache_event,upload_done_event],[]
        
        
        
    def upload_done(self, content_id, peer_id, type, time_cur, yng_request):
        """
        Method utk report upload done
        """
        self.upload_state = 'IDLE'
        #print 'masuk'
        self.cdn.receive_report_from_peer(peer_id, content_id, 'IDLE', time_cur, yng_request)
        return [],[]
        
        
            
    def get_content(self, content_id, time_cur, yng_request):
        """
        ambil content dari peer
        """
        #peer_uploader = self.id
        #upload ke peer lalu lapor state jadi UPLOADING
        self.upload_state = 'UPLOADING'
        self.log_replica[content_id]['t_di_access'].append( (time_cur,yng_request) )
        self.cdn.receive_report_from_peer(self.id, content_id, 'UPLOADING', time_cur, yng_request)
        #print 'PEER',self.id, 'selfcache' , self.cache_entries[content_id]
        return self.cache_entries[content_id]
        
    
    #def clean_up_cache(self):
    #    for k,v in self.log_replica.iteritems():
            #k = key = content_id
            #lapor remove ke CDN
            #self.cdn.receive_report_from_peer(self.id, k, 'REMOVE_CACHE', 0, self.id, v)
            #
            #temp_list.append(v)
    #       self.cdn.receive_report_from_peer(self.id, k, 'REMOVE_CACHE_AKHIR', 0, self.id, v)


    def cache_content_cdn(self, content_id, content, time_cur):
        """
        cache the content in the peer
        and report to CDN
        karena membutuhkan cdn class dng method report_to_cdn
        """
        size_video_baru = content[2]
        jumlah=sum(self.cache_size.values())
        content_baru=[ content[0], content[1], content[2], time_cur, content[4] ]

        #posisi disini diganti dng estimasi t
        #kalau belum pernah diakses akan estimasi dari t
        #kalau sudah pernah diakses akan estimasi dar viewrate
        #posisi_minggu_video_baru = content[4]
        yng_request=self.id

        cek_pertama_kali = self.cdn.get_first_time_requested(content_id)

        #check capacity
        if (jumlah+size_video_baru) <= 500: #kurang dari 500MB langsung dicache
            #langsung masuk dicache
            content_baru=[ content[0], content[1], content[2], time_cur, content[4] ]
            self.cache_entries[content_id]=content_baru
            self.cache_size[content_id]=size_video_baru
            #video_id:{ 'self-id’: self.id,  =’t_di_cache’=float,  ’t_di_remove’=float, t_di_access=[(t,peer_origin),…] }
            self.log_replica[content_id]={'content-id': content_id, 'peer-id': self.id, 't_di_cache': time_cur, 't_di_access': [] , 't_di_remove':0 }
            self.cdn.receive_report_from_peer(self.id, content_id, 'CACHE', time_cur, yng_request)


        else:
            #kalau cache penuh cek apakah video ini pertama kali diakses?

            minggu,second = divmod(time_cur, 7*24*60*60)

            if cek_pertama_kali == 1:
                #positif pertama kali diakses
                #gunakan tebakan t minggu
                tebakan_minggu = self.cdn.catatan_weekly(minggu)
                if tebakan_minggu == 1:
                    #positif dalam posisi peak
                    #gunakan utility function peak time
                    #hitung p utk video baru
                    #ambil nilai t_ir, n_ir, a_ir utk video baru
                    n_ir = float(self.cdn.get_number_requested_video(content_id))
                    t_ir = float(self.cdn.get_video_last_time_requested(content_id))
                    a_i = content[1]
                    kanan = (1.0)/abs(time_cur - t_ir)
                    kiri = (n_ir)/abs(t_ir - a_i)
                    P_i = min(kiri,kanan)
                    #ambil catatan P_max semua video yng ada di sistem dari cdn 
                    P_max = self.cdn.hitung_p_max(time_cur)

                    #ambil catatan jumlah replica content_id ini dari cdn
                    r = self.cdn.get_replica(content_id) 
                    r = float(r)

                    if r == 0:
                        utility_video_baru = 0.0
                    elif P_i == 0:
                        #hitung utility video baru
                        utility_video_baru = abs(math.log(P_max))/r
                    else:
                        utility_video_baru = abs(math.log(P_max)) - abs(math.log(P_i))/r
                        utility_video_baru = abs(utility_video_baru)
                    #hitung p utk video didalam cache
                    #ambil content id yng sudah ada didalam cache
                    temp_2={}
                    utility=0
                    content_id_dicache=self.cache_entries.keys()
                    for con in content_id_dicache:
                        #ambil nilai t_ir, n_ir, a_ir utk con (dalam cache)
                        n_ir = float(self.cdn.get_number_requested_video(con))
                        t_ir = float(self.cdn.get_video_last_time_requested(con))
                        a_i = self.cdn.get_upload_time(con)
                        kanan = (1.0)/abs(time_cur - t_ir)
                        kiri = (n_ir)/abs(t_ir - a_i)
                        P_i = min(kiri,kanan)

                        #ambil catatan jumlah replica tiap con dari cdn
                        r = self.cdn.get_replica(con)
                        r = float(r)

                        if r == 0:
                            utility = 0.0
                        elif P_i == 0:
                            utility = abs(math.log(P_max))/r
                        else:
                            utility = abs(math.log(P_max)) - abs(math.log(P_i)) / r
                            utility = abs(utility)
                        temp_2[con]=utility
                    #cari minimum utility video didalam cache
                    #video_id_dng_u_min = min(temp_1, key = lambda x: temp_1.get(x) )
                    #sorted dictionary by values (utility):
                    list_sorted_utility = sorted(temp_2.items(), key=itemgetter(1))
                    #hasilnya berupa list of tuple
                    utility_min_video_dicache = list_sorted_utility[0][1]
                
                    #bandingkan utility video didalam cache dng video yng akan masuk
                    #bila utility min dalam cache lebih kecil:
                    if utility_min_video_dicache < utility_video_baru:
                        jumlah=sum(self.cache_size.values())
                        #disini secaraiterative hapus cache
                        while (jumlah+size_video_baru) >= 500: #selama jumlah >= 500 hapus terus cache
                            tup = list_sorted_utility.pop(0)
                            video_id_dng_u_min = tup[0]
                            video_size_dng_u_min = tup[1]
                            del self.cache_size[video_id_dng_u_min]
                            del self.cache_entries[video_id_dng_u_min]
                            self.log_replica[video_id_dng_u_min]['t_di_remove']=time_cur
                            self.cdn.receive_report_from_peer(self.id, video_id_dng_u_min, 'REMOVE_CACHE', time_cur, yng_request,self.log_replica[video_id_dng_u_min])
                            del self.log_replica[video_id_dng_u_min]
                            jumlah=sum(self.cache_size.values())
                        #setelah jumlah+size_video_baru <= 500 maka
                        #cache utk video baru yng masuk
                        self.cache_entries[content_id]=content_baru
                        self.cache_size[content_id]=size_video_baru
                        self.log_replica[content_id]={'content-id': content_id, 'peer-id': self.id, 't_di_cache': time_cur, 't_di_access': [] , 't_di_remove':0 }
                        self.cdn.receive_report_from_peer(self.id, content_id, 'CACHE', time_cur, yng_request)
                    else:
                        pass
                else:
                    #dalam posisi before peak/after peak
                    #gunakan utility function before/after
                    t_ir = float(self.cdn.get_video_last_time_requested(content_id))
                    n_ir = float(self.cdn.get_number_requested_video(content_id))
                    a_i = content[1]
                    kanan = (1.0)/abs(time_cur - t_ir)
                    kiri = (n_ir)/abs(t_ir - a_i)
                    P_i = min(kiri,kanan)
                    #ambil catatan P_min semua video yng ada di sistem dari cdn
                    P_min = self.cdn.hitung_p_min(time_cur)
                    #ambil catatan jumlah replica content_id ini dari cdn
                    r = self.cdn.get_replica(content_id) 
                    r = float(r)
                    if r == 0:
                        utility_video_baru = 0.0
                    elif P_min == 0:
                        #hitung utility video baru
                        utility_video_baru = abs(math.log(P_i))/r 
                    else:
                        utility_video_baru = abs(math.log(P_i)) - abs(math.log(P_min))/r
                        utility_video_baru = abs(utility_video_baru)
                    #hitung p utk video didalam cache
                    #ambil content id yng sudah ada didalam cache
                    temp_1={}
                    utility=0
                    content_id_dicache=self.cache_entries.keys()
                    for con in content_id_dicache:
                        #ambil nilai t_ir, n_ir, a_ir
                        n_ir = float(self.cdn.get_number_requested_video(con))
                        t_ir = float(self.cdn.get_video_last_time_requested(con))
                        a_i = self.cdn.get_upload_time(con)
                        kanan = (1.0)/abs(time_cur - t_ir)
                        kiri = (n_ir)/abs(t_ir - a_i)
                        P_i = min(kiri,kanan)
                        #ambil catatan jumlah replica tiap con dari cdn
                        r = self.cdn.get_replica(con)
                        r = float(r)
                        if r == 0:
                            utility=0.0
                        elif P_min == 0:
                            utility = abs(math.log(P_i))/r
                            utility = abs(utility)
                        else:
                            utility = abs(math.log(P_i)) - abs(math.log(P_min)) / r
                            utility = abs(utility)
                        temp_1[con]=utility
                    #cari minimum utility video didalam cache
                    #sorted dictionary by values (utility):
                    list_sorted_utility = sorted(temp_1.items(), key=itemgetter(1))
                    utility_min_video_dicache = list_sorted_utility[0][1]
                    #bandingkan utility video didalam cache dng video yng akan masuk
                    #bila p_min dalam cache lebih kecil:
                    if utility_min_video_dicache < utility_video_baru:
                        jumlah=sum(self.cache_size.values())
                        #disini secaraiterative hapus cache
                        while (jumlah+size_video_baru) >= 500: #selama jumlah >= 500 hapus terus cache
                            tup = list_sorted_utility.pop(0)
                            video_id_dng_u_min = tup[0]
                            video_size_dng_u_min = tup[1]
                            del self.cache_size[video_id_dng_u_min]
                            del self.cache_entries[video_id_dng_u_min]
                            self.log_replica[video_id_dng_u_min]['t_di_remove']=time_cur
                            self.cdn.receive_report_from_peer(self.id, video_id_dng_u_min, 'REMOVE_CACHE', time_cur, yng_request,self.log_replica[video_id_dng_u_min])
                            del self.log_replica[video_id_dng_u_min]
                            jumlah=sum(self.cache_size.values())
                        #setelah jumlah <= 500 maka
                        #cache utk video baru yng masuk
                        self.cache_entries[content_id]=content_baru
                        self.cache_size[content_id]=size_video_baru
                        self.log_replica[content_id]={'content-id': content_id, 'peer-id': self.id, 't_di_cache': time_cur, 't_di_access': [] , 't_di_remove':0 }
                        self.cdn.receive_report_from_peer(self.id, content_id, 'CACHE', time_cur, yng_request)
                    else:
                        pass


            else:
                #tdk pertama kali diakses
                #gunakan tebakan viewrate
                hasil = self.cdn.estimasi_vr(time_cur,content_id)
                if hasil == 0:
                    #peak time
                    #gunakan utility function peak-time
                    #positif dalam posisi peak
                    #gunakan utility function peak time
                    #hitung p utk video baru
                    #ambil nilai t_ir, n_ir, a_ir utk video baru
                    n_ir = float(self.cdn.get_number_requested_video(content_id))
                    t_ir = float(self.cdn.get_video_last_time_requested(content_id))
                    a_i = content[1]
                    kanan = (1.0)/abs(time_cur - t_ir)
                    kiri = (n_ir)/abs(t_ir - a_i)
                    P_i = min(kiri,kanan)
                    #ambil catatan P_max semua video yng ada di sistem dari cdn 
                    P_max = self.cdn.hitung_p_max(time_cur)

                    #ambil catatan jumlah replica content_id ini dari cdn
                    r = self.cdn.get_replica(content_id) 
                    r = float(r)

                    if r == 0:
                        utility_video_baru = 0.0
                    elif P_i == 0:
                        #hitung utility video baru
                        utility_video_baru = abs(math.log(P_max))/r
                    else:
                        utility_video_baru = abs(math.log(P_max)) - abs(math.log(P_i))/r
                        utility_video_baru = abs(utility_video_baru)
                    #hitung p utk video didalam cache
                    #ambil content id yng sudah ada didalam cache
                    temp_2={}
                    utility=0
                    content_id_dicache=self.cache_entries.keys()
                    for con in content_id_dicache:
                        #ambil nilai t_ir, n_ir, a_ir utk con (dalam cache)
                        n_ir = float(self.cdn.get_number_requested_video(con))
                        t_ir = float(self.cdn.get_video_last_time_requested(con))
                        a_i = self.cdn.get_upload_time(con)
                        kanan = (1.0)/abs(time_cur - t_ir)
                        kiri = (n_ir)/abs(t_ir - a_i)
                        P_i = min(kiri,kanan)

                        #ambil catatan jumlah replica tiap con dari cdn
                        r = self.cdn.get_replica(con)
                        r = float(r)

                        if r == 0:
                            utility = 0.0
                        elif P_i == 0:
                            utility = abs(math.log(P_max))/r
                        else:
                            utility = abs(math.log(P_max)) - abs(math.log(P_i)) / r
                            utility = abs(utility)
                        temp_2[con]=utility
                    #cari minimum utility video didalam cache
                    #video_id_dng_u_min = min(temp_1, key = lambda x: temp_1.get(x) )
                    #sorted dictionary by values (utility):
                    list_sorted_utility = sorted(temp_2.items(), key=itemgetter(1))
                    #hasilnya berupa list of tuple
                    utility_min_video_dicache = list_sorted_utility[0][1]
                
                    #bandingkan utility video didalam cache dng video yng akan masuk
                    #bila utility min dalam cache lebih kecil:
                    if utility_min_video_dicache < utility_video_baru:
                        jumlah=sum(self.cache_size.values())
                        #disini secaraiterative hapus cache
                        while (jumlah+size_video_baru) >= 500: #selama jumlah >= 500 hapus terus cache
                            tup = list_sorted_utility.pop(0)
                            video_id_dng_u_min = tup[0]
                            video_size_dng_u_min = tup[1]
                            del self.cache_size[video_id_dng_u_min]
                            del self.cache_entries[video_id_dng_u_min]
                            self.log_replica[video_id_dng_u_min]['t_di_remove']=time_cur
                            self.cdn.receive_report_from_peer(self.id, video_id_dng_u_min, 'REMOVE_CACHE', time_cur, yng_request,self.log_replica[video_id_dng_u_min])
                            del self.log_replica[video_id_dng_u_min]
                            jumlah=sum(self.cache_size.values())
                        #setelah jumlah+size_video_baru <= 500 maka
                        #cache utk video baru yng masuk
                        self.cache_entries[content_id]=content_baru
                        self.cache_size[content_id]=size_video_baru
                        self.log_replica[content_id]={'content-id': content_id, 'peer-id': self.id, 't_di_cache': time_cur, 't_di_access': [] , 't_di_remove':0 }
                        self.cdn.receive_report_from_peer(self.id, content_id, 'CACHE', time_cur, yng_request)
                    else:
                        pass

                else:
                    #before atau after
                    #gunakan utility function before/after
                    #dalam posisi before peak/after peak
                    #gunakan utility function before/after
                    t_ir = float(self.cdn.get_video_last_time_requested(content_id))
                    n_ir = float(self.cdn.get_number_requested_video(content_id))
                    a_i = content[1]
                    kanan = (1.0)/abs(time_cur - t_ir)
                    kiri = (n_ir)/abs(t_ir - a_i)
                    P_i = min(kiri,kanan)
                    #ambil catatan P_min semua video yng ada di sistem dari cdn
                    P_min = self.cdn.hitung_p_min(time_cur)
                    #ambil catatan jumlah replica content_id ini dari cdn
                    r = self.cdn.get_replica(content_id) 
                    r = float(r)
                    if r == 0:
                        utility_video_baru = 0.0
                    elif P_min == 0:
                        #hitung utility video baru
                        utility_video_baru = abs(math.log(P_i))/r 
                    else:
                        utility_video_baru = abs(math.log(P_i)) - abs(math.log(P_min))/r
                        utility_video_baru = abs(utility_video_baru)
                    #hitung p utk video didalam cache
                    #ambil content id yng sudah ada didalam cache
                    temp_1={}
                    utility=0
                    content_id_dicache=self.cache_entries.keys()
                    for con in content_id_dicache:
                        #ambil nilai t_ir, n_ir, a_ir
                        n_ir = float(self.cdn.get_number_requested_video(con))
                        t_ir = float(self.cdn.get_video_last_time_requested(con))
                        a_i = self.cdn.get_upload_time(con)
                        kanan = (1.0)/abs(time_cur - t_ir)
                        kiri = (n_ir)/abs(t_ir - a_i)
                        P_i = min(kiri,kanan)
                        #ambil catatan jumlah replica tiap con dari cdn
                        r = self.cdn.get_replica(con)
                        r = float(r)
                        if r == 0:
                            utility=0.0
                        elif P_min == 0:
                            utility = abs(math.log(P_i))/r
                            utility = abs(utility)
                        else:
                            utility = abs(math.log(P_i)) - abs(math.log(P_min)) / r
                            utility = abs(utility)
                        temp_1[con]=utility
                    #cari minimum utility video didalam cache
                    #sorted dictionary by values (utility):
                    list_sorted_utility = sorted(temp_1.items(), key=itemgetter(1))
                    utility_min_video_dicache = list_sorted_utility[0][1]
                    #bandingkan utility video didalam cache dng video yng akan masuk
                    #bila p_min dalam cache lebih kecil:
                    if utility_min_video_dicache < utility_video_baru:
                        jumlah=sum(self.cache_size.values())
                        #disini secaraiterative hapus cache
                        while (jumlah+size_video_baru) >= 500: #selama jumlah >= 500 hapus terus cache
                            tup = list_sorted_utility.pop(0)
                            video_id_dng_u_min = tup[0]
                            video_size_dng_u_min = tup[1]
                            del self.cache_size[video_id_dng_u_min]
                            del self.cache_entries[video_id_dng_u_min]
                            self.log_replica[video_id_dng_u_min]['t_di_remove']=time_cur
                            self.cdn.receive_report_from_peer(self.id, video_id_dng_u_min, 'REMOVE_CACHE', time_cur, yng_request,self.log_replica[video_id_dng_u_min])
                            del self.log_replica[video_id_dng_u_min]
                            jumlah=sum(self.cache_size.values())
                        #setelah jumlah <= 500 maka
                        #cache utk video baru yng masuk
                        self.cache_entries[content_id]=content_baru
                        self.cache_size[content_id]=size_video_baru
                        self.log_replica[content_id]={'content-id': content_id, 'peer-id': self.id, 't_di_cache': time_cur, 't_di_access': [] , 't_di_remove':0 }
                        self.cdn.receive_report_from_peer(self.id, content_id, 'CACHE', time_cur, yng_request)
                    else:
                        pass
        return [],[]


    def cache_content(self, content_id, content, time_cur):
        """
        cache the content in the peer
        and report to CDN
        karena membutuhkan cdn class dng method report_to_cdn
        """
        size_video_baru = content[2]
        jumlah=sum(self.cache_size.values())
        content_baru=[ content[0], content[1], content[2], time_cur, content[4] ]

        #posisi disini diganti dng estimasi t
        #kalau belum pernah diakses akan estimasi dari t
        #kalau sudah pernah diakses akan estimasi dar viewrate
        #posisi_minggu_video_baru = content[4]
        yng_request=self.id

        cek_pertama_kali = self.cdn.get_first_time_requested(content_id)

        #check capacity
        if (jumlah+size_video_baru) <= 500: #kurang dari 500MB langsung dicache
            #langsung masuk dicache
            content_baru=[ content[0], content[1], content[2], time_cur, content[4] ]
            self.cache_entries[content_id]=content_baru
            self.cache_size[content_id]=size_video_baru
            #video_id:{ 'self-id’: self.id,  =’t_di_cache’=float,  ’t_di_remove’=float, t_di_access=[(t,peer_origin),…] }
            self.log_replica[content_id]={'content-id': content_id, 'peer-id': self.id, 't_di_cache': time_cur, 't_di_access': [] , 't_di_remove':0 }
            self.cdn.receive_report_from_peer(self.id, content_id, 'CACHE', time_cur, yng_request)


        else:
            #kalau cache penuh cek apakah video ini pertama kali diakses?

            minggu,second = divmod(time_cur, 7*24*60*60)

            if cek_pertama_kali == 1:
                #positif pertama kali diakses
                #gunakan tebakan t minggu
                tebakan_minggu = self.cdn.catatan_weekly(minggu)
                if tebakan_minggu == 1:
                    #positif dalam posisi peak
                    #gunakan utility function peak time
                    #hitung p utk video baru
                    #ambil nilai t_ir, n_ir, a_ir utk video baru
                    n_ir = float(self.cdn.get_number_requested_video(content_id))
                    t_ir = float(self.cdn.get_video_last_time_requested(content_id))
                    a_i = content[1]
                    kanan = (1.0)/abs(time_cur - t_ir)
                    kiri = (n_ir)/abs(t_ir - a_i)
                    P_i = min(kiri,kanan)
                    #ambil catatan P_max semua video yng ada di sistem dari cdn 
                    P_max = self.cdn.hitung_p_max(time_cur)

                    #ambil catatan jumlah replica content_id ini dari cdn
                    r = self.cdn.get_replica(content_id) 
                    r = float(r)

                    if r == 0:
                        utility_video_baru = 0.0
                    elif P_i == 0:
                        #hitung utility video baru
                        utility_video_baru = abs(math.log(P_max))/r
                    else:
                        utility_video_baru = abs(math.log(P_max)) - abs(math.log(P_i))/r
                        utility_video_baru = abs(utility_video_baru)
                    #hitung p utk video didalam cache
                    #ambil content id yng sudah ada didalam cache
                    temp_2={}
                    utility=0
                    content_id_dicache=self.cache_entries.keys()
                    for con in content_id_dicache:
                        #ambil nilai t_ir, n_ir, a_ir utk con (dalam cache)
                        n_ir = float(self.cdn.get_number_requested_video(con))
                        t_ir = float(self.cdn.get_video_last_time_requested(con))
                        a_i = self.cdn.get_upload_time(con)
                        kanan = (1.0)/abs(time_cur - t_ir)
                        kiri = (n_ir)/abs(t_ir - a_i)
                        P_i = min(kiri,kanan)

                        #ambil catatan jumlah replica tiap con dari cdn
                        r = self.cdn.get_replica(con)
                        r = float(r)

                        if r == 0:
                            utility = 0.0
                        elif P_i == 0:
                            utility = abs(math.log(P_max))/r
                        else:
                            utility = abs(math.log(P_max)) - abs(math.log(P_i)) / r
                            utility = abs(utility)
                        temp_2[con]=utility
                    #cari minimum utility video didalam cache
                    #video_id_dng_u_min = min(temp_1, key = lambda x: temp_1.get(x) )
                    #sorted dictionary by values (utility):
                    list_sorted_utility = sorted(temp_2.items(), key=itemgetter(1))
                    #hasilnya berupa list of tuple
                    utility_min_video_dicache = list_sorted_utility[0][1]
                
                    #bandingkan utility video didalam cache dng video yng akan masuk
                    #bila utility min dalam cache lebih kecil:
                    if utility_min_video_dicache < utility_video_baru:
                        jumlah=sum(self.cache_size.values())
                        #disini secaraiterative hapus cache
                        while (jumlah+size_video_baru) >= 500: #selama jumlah >= 500 hapus terus cache
                            tup = list_sorted_utility.pop(0)
                            video_id_dng_u_min = tup[0]
                            video_size_dng_u_min = tup[1]
                            del self.cache_size[video_id_dng_u_min]
                            del self.cache_entries[video_id_dng_u_min]
                            self.log_replica[video_id_dng_u_min]['t_di_remove']=time_cur
                            self.cdn.receive_report_from_peer(self.id, video_id_dng_u_min, 'REMOVE_CACHE', time_cur, yng_request,self.log_replica[video_id_dng_u_min])
                            del self.log_replica[video_id_dng_u_min]
                            jumlah=sum(self.cache_size.values())
                        #setelah jumlah+size_video_baru <= 500 maka
                        #cache utk video baru yng masuk
                        self.cache_entries[content_id]=content_baru
                        self.cache_size[content_id]=size_video_baru
                        self.log_replica[content_id]={'content-id': content_id, 'peer-id': self.id, 't_di_cache': time_cur, 't_di_access': [] , 't_di_remove':0 }
                        self.cdn.receive_report_from_peer(self.id, content_id, 'CACHE', time_cur, yng_request)
                    else:
                        pass
                else:
                    #dalam posisi before peak/after peak
                    #gunakan utility function before/after
                    t_ir = float(self.cdn.get_video_last_time_requested(content_id))
                    n_ir = float(self.cdn.get_number_requested_video(content_id))
                    a_i = content[1]
                    kanan = (1.0)/abs(time_cur - t_ir)
                    kiri = (n_ir)/abs(t_ir - a_i)
                    P_i = min(kiri,kanan)
                    #ambil catatan P_min semua video yng ada di sistem dari cdn
                    P_min = self.cdn.hitung_p_min(time_cur)
                    #ambil catatan jumlah replica content_id ini dari cdn
                    r = self.cdn.get_replica(content_id) 
                    r = float(r)
                    if r == 0:
                        utility_video_baru = 0.0
                    elif P_min == 0:
                        #hitung utility video baru
                        utility_video_baru = abs(math.log(P_i))/r 
                    else:
                        utility_video_baru = abs(math.log(P_i)) - abs(math.log(P_min))/r
                        utility_video_baru = abs(utility_video_baru)
                    #hitung p utk video didalam cache
                    #ambil content id yng sudah ada didalam cache
                    temp_1={}
                    utility=0
                    content_id_dicache=self.cache_entries.keys()
                    for con in content_id_dicache:
                        #ambil nilai t_ir, n_ir, a_ir
                        n_ir = float(self.cdn.get_number_requested_video(con))
                        t_ir = float(self.cdn.get_video_last_time_requested(con))
                        a_i = self.cdn.get_upload_time(con)
                        kanan = (1.0)/abs(time_cur - t_ir)
                        kiri = (n_ir)/abs(t_ir - a_i)
                        P_i = min(kiri,kanan)
                        #ambil catatan jumlah replica tiap con dari cdn
                        r = self.cdn.get_replica(con)
                        r = float(r)
                        if r == 0:
                            utility=0.0
                        elif P_min == 0:
                            utility = abs(math.log(P_i))/r
                            utility = abs(utility)
                        else:
                            utility = abs(math.log(P_i)) - abs(math.log(P_min)) / r
                            utility = abs(utility)
                        temp_1[con]=utility
                    #cari minimum utility video didalam cache
                    #sorted dictionary by values (utility):
                    list_sorted_utility = sorted(temp_1.items(), key=itemgetter(1))
                    utility_min_video_dicache = list_sorted_utility[0][1]
                    #bandingkan utility video didalam cache dng video yng akan masuk
                    #bila p_min dalam cache lebih kecil:
                    if utility_min_video_dicache < utility_video_baru:
                        jumlah=sum(self.cache_size.values())
                        #disini secaraiterative hapus cache
                        while (jumlah+size_video_baru) >= 500: #selama jumlah >= 500 hapus terus cache
                            tup = list_sorted_utility.pop(0)
                            video_id_dng_u_min = tup[0]
                            video_size_dng_u_min = tup[1]
                            del self.cache_size[video_id_dng_u_min]
                            del self.cache_entries[video_id_dng_u_min]
                            self.log_replica[video_id_dng_u_min]['t_di_remove']=time_cur
                            self.cdn.receive_report_from_peer(self.id, video_id_dng_u_min, 'REMOVE_CACHE', time_cur, yng_request,self.log_replica[video_id_dng_u_min])
                            del self.log_replica[video_id_dng_u_min]
                            jumlah=sum(self.cache_size.values())
                        #setelah jumlah <= 500 maka
                        #cache utk video baru yng masuk
                        self.cache_entries[content_id]=content_baru
                        self.cache_size[content_id]=size_video_baru
                        self.log_replica[content_id]={'content-id': content_id, 'peer-id': self.id, 't_di_cache': time_cur, 't_di_access': [] , 't_di_remove':0 }
                        self.cdn.receive_report_from_peer(self.id, content_id, 'CACHE', time_cur, yng_request)
                    else:
                        pass

            else:
                #tdk pertama kali diakses
                #gunakan tebakan viewrate
                hasil = self.cdn.estimasi_vr(time_cur,content_id)
                if hasil == 0:
                    #peak time
                    #gunakan utility function peak-time
                    #positif dalam posisi peak
                    #gunakan utility function peak time
                    #hitung p utk video baru
                    #ambil nilai t_ir, n_ir, a_ir utk video baru
                    n_ir = float(self.cdn.get_number_requested_video(content_id))
                    t_ir = float(self.cdn.get_video_last_time_requested(content_id))
                    a_i = content[1]
                    kanan = (1.0)/abs(time_cur - t_ir)
                    kiri = (n_ir)/abs(t_ir - a_i)
                    P_i = min(kiri,kanan)
                    #ambil catatan P_max semua video yng ada di sistem dari cdn 
                    P_max = self.cdn.hitung_p_max(time_cur)

                    #ambil catatan jumlah replica content_id ini dari cdn
                    r = self.cdn.get_replica(content_id) 
                    r = float(r)

                    if r == 0:
                        utility_video_baru = 0.0
                    elif P_i == 0:
                        #hitung utility video baru
                        utility_video_baru = abs(math.log(P_max))/r
                    else:
                        utility_video_baru = abs(math.log(P_max)) - abs(math.log(P_i))/r
                        utility_video_baru = abs(utility_video_baru)
                    #hitung p utk video didalam cache
                    #ambil content id yng sudah ada didalam cache
                    temp_2={}
                    utility=0
                    content_id_dicache=self.cache_entries.keys()
                    for con in content_id_dicache:
                        #ambil nilai t_ir, n_ir, a_ir utk con (dalam cache)
                        n_ir = float(self.cdn.get_number_requested_video(con))
                        t_ir = float(self.cdn.get_video_last_time_requested(con))
                        a_i = self.cdn.get_upload_time(con)
                        kanan = (1.0)/abs(time_cur - t_ir)
                        kiri = (n_ir)/abs(t_ir - a_i)
                        P_i = min(kiri,kanan)

                        #ambil catatan jumlah replica tiap con dari cdn
                        r = self.cdn.get_replica(con)
                        r = float(r)

                        if r == 0:
                            utility = 0.0
                        elif P_i == 0:
                            utility = abs(math.log(P_max))/r
                        else:
                            utility = abs(math.log(P_max)) - abs(math.log(P_i)) / r
                            utility = abs(utility)
                        temp_2[con]=utility
                    #cari minimum utility video didalam cache
                    #video_id_dng_u_min = min(temp_1, key = lambda x: temp_1.get(x) )
                    #sorted dictionary by values (utility):
                    list_sorted_utility = sorted(temp_2.items(), key=itemgetter(1))
                    #hasilnya berupa list of tuple
                    utility_min_video_dicache = list_sorted_utility[0][1]
                
                    #bandingkan utility video didalam cache dng video yng akan masuk
                    #bila utility min dalam cache lebih kecil:
                    if utility_min_video_dicache < utility_video_baru:
                        jumlah=sum(self.cache_size.values())
                        #disini secaraiterative hapus cache
                        while (jumlah+size_video_baru) >= 500: #selama jumlah >= 500 hapus terus cache
                            tup = list_sorted_utility.pop(0)
                            video_id_dng_u_min = tup[0]
                            video_size_dng_u_min = tup[1]
                            del self.cache_size[video_id_dng_u_min]
                            del self.cache_entries[video_id_dng_u_min]
                            self.log_replica[video_id_dng_u_min]['t_di_remove']=time_cur
                            self.cdn.receive_report_from_peer(self.id, video_id_dng_u_min, 'REMOVE_CACHE', time_cur, yng_request,self.log_replica[video_id_dng_u_min])
                            del self.log_replica[video_id_dng_u_min]
                            jumlah=sum(self.cache_size.values())
                        #setelah jumlah+size_video_baru <= 500 maka
                        #cache utk video baru yng masuk
                        self.cache_entries[content_id]=content_baru
                        self.cache_size[content_id]=size_video_baru
                        self.log_replica[content_id]={'content-id': content_id, 'peer-id': self.id, 't_di_cache': time_cur, 't_di_access': [] , 't_di_remove':0 }
                        self.cdn.receive_report_from_peer(self.id, content_id, 'CACHE', time_cur, yng_request)
                    else:
                        pass

                else:
                    #before atau after
                    #gunakan utility function before/after
                    #dalam posisi before peak/after peak
                    #gunakan utility function before/after
                    t_ir = float(self.cdn.get_video_last_time_requested(content_id))
                    n_ir = float(self.cdn.get_number_requested_video(content_id))
                    a_i = content[1]
                    kanan = (1.0)/abs(time_cur - t_ir)
                    kiri = (n_ir)/abs(t_ir - a_i)
                    P_i = min(kiri,kanan)
                    #ambil catatan P_min semua video yng ada di sistem dari cdn
                    P_min = self.cdn.hitung_p_min(time_cur)
                    #ambil catatan jumlah replica content_id ini dari cdn
                    r = self.cdn.get_replica(content_id) 
                    r = float(r)
                    if r == 0:
                        utility_video_baru = 0.0
                    elif P_min == 0:
                        #hitung utility video baru
                        utility_video_baru = abs(math.log(P_i))/r 
                    else:
                        utility_video_baru = abs(math.log(P_i)) - abs(math.log(P_min))/r
                        utility_video_baru = abs(utility_video_baru)
                    #hitung p utk video didalam cache
                    #ambil content id yng sudah ada didalam cache
                    temp_1={}
                    utility=0
                    content_id_dicache=self.cache_entries.keys()
                    for con in content_id_dicache:
                        #ambil nilai t_ir, n_ir, a_ir
                        n_ir = float(self.cdn.get_number_requested_video(con))
                        t_ir = float(self.cdn.get_video_last_time_requested(con))
                        a_i = self.cdn.get_upload_time(con)
                        kanan = (1.0)/abs(time_cur - t_ir)
                        kiri = (n_ir)/abs(t_ir - a_i)
                        P_i = min(kiri,kanan)
                        #ambil catatan jumlah replica tiap con dari cdn
                        r = self.cdn.get_replica(con)
                        r = float(r)
                        if r == 0:
                            utility=0.0
                        elif P_min == 0:
                            utility = abs(math.log(P_i))/r
                            utility = abs(utility)
                        else:
                            utility = abs(math.log(P_i)) - abs(math.log(P_min)) / r
                            utility = abs(utility)
                        temp_1[con]=utility
                    #cari minimum utility video didalam cache
                    #sorted dictionary by values (utility):
                    list_sorted_utility = sorted(temp_1.items(), key=itemgetter(1))
                    utility_min_video_dicache = list_sorted_utility[0][1]
                    #bandingkan utility video didalam cache dng video yng akan masuk
                    #bila p_min dalam cache lebih kecil:
                    if utility_min_video_dicache < utility_video_baru:
                        jumlah=sum(self.cache_size.values())
                        #disini secaraiterative hapus cache
                        while (jumlah+size_video_baru) >= 500: #selama jumlah >= 500 hapus terus cache
                            tup = list_sorted_utility.pop(0)
                            video_id_dng_u_min = tup[0]
                            video_size_dng_u_min = tup[1]
                            del self.cache_size[video_id_dng_u_min]
                            del self.cache_entries[video_id_dng_u_min]
                            self.log_replica[video_id_dng_u_min]['t_di_remove']=time_cur
                            self.cdn.receive_report_from_peer(self.id, video_id_dng_u_min, 'REMOVE_CACHE', time_cur, yng_request,self.log_replica[video_id_dng_u_min])
                            del self.log_replica[video_id_dng_u_min]
                            jumlah=sum(self.cache_size.values())
                        #setelah jumlah <= 500 maka
                        #cache utk video baru yng masuk
                        self.cache_entries[content_id]=content_baru
                        self.cache_size[content_id]=size_video_baru
                        self.log_replica[content_id]={'content-id': content_id, 'peer-id': self.id, 't_di_cache': time_cur, 't_di_access': [] , 't_di_remove':0 }
                        self.cdn.receive_report_from_peer(self.id, content_id, 'CACHE', time_cur, yng_request)
                    else:
                        pass
        return [],[]



    
    def remove_content(self, content_id):
        """
        menghapus cache
        """
        #print self.id, content_id
        yng_request=self.id
        self.cdn.receive_report_from_peer(self.id, content_id, 'REMOVE_CACHE',  time_cur, yng_request)
        #print self.id, content_id
        del self.cache_size[content_id]
        del self.cache_entries[content_id]
        return [],[]


    def remove_content_extend(self, content_id):
        """
        menghapus cache
        """
        #print self.id, content_id
        yng_request=self.id
        self.cdn.receive_report_from_peer(self.id, content_id, 'REMOVE_CACHE', time_cur, yng_request)
        #print self.id, content_id
        del self.cache_entries[content_id]
        return [],[]

    def remove_content_old(self, content_id):
        """
        menghapus cache
        """
        #print self.id, content_id
        #self.cdn.receive_report_from_peer(self.id, content_id, 'REMOVE_CACHE')
        #print self.id, content_id
        #del self.cache_entries[content_id]
        return [],[]


    
    def change_content_expiry_time(self, content, time_cur):
        """
        mengubah content expiry time, misalnya saat upload content yang akan expire waktu content sedang diupload
        """
        content_id = content[0]
        time_lama = self.cache_entries[content_id][3]

        #hitung durasi dan time_baru
        #contoh: {0: [0, 5.0775931306168065, 499.0, 3600.0]}
        durasi = (content[2]*8)/(self.dn_bw/1000.0)
        time_baru = time_cur + durasi
        #print '--->', time_cur, content[0], content[1], content[2], self.dn_bw, durasi, time_baru, time_lama
        if time_baru > time_lama:
            new_expire_event = event.Event(event.REMOVE_CONTENT, time_baru, self, self.remove_content_extend, [content[0]])
            old_expire_event = event.Event(event.REMOVE_CONTENT, time_lama, self, self.remove_content, [content[0]])
            #print 'change',  new_expire_event, old_expire_event
            #update cache entries dng time baru.
            self.cache_entries[content_id][3] = time_baru
            return [new_expire_event, old_expire_event]
        else:
            return None, None
        
