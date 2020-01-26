from bs4 import BeautifulSoup
import requests
import os
import time
import locale
import logging
import csv
import pandas as pd
from docx import Document
import psycopg2
import sched, time
from docx import Document
import json
import getpass
import sched

conn = psycopg2.connect(host="localhost", database="foot", user="postgres", password="digifab", port=5432)
curseur=conn.cursor()

locale.setlocale(locale.LC_TIME,'')
date =time.strftime('%Y-%m-%d_%H:%M:%S')
logging.basicConfig(filename='/Users/romelus/Documents/Sites/StatFoot/fichier_log/fichier.log',level=logging.DEBUG, format='%(asctime)s:%(name)s:%(message)s')

TIME= 200
s = sched.scheduler(time.time, time.sleep)

def traitements():

    def dataBrut(source, nomFile):
        '''*-------- Création du fichier de données brut --------*'''

        doc = Document()
        doc.add_paragraph(str(source))
        doc.save('/Users/romelus/Documents/Sites/StatFoot/brut_data/'+str(time.strftime('%Y-%m-%d_%H:%M:%S'))+'-'+nomFile+'.docx')
        logging.debug('Données brut sauvegardé : '+nomFile)


    def metaData(link, nomFile):
        '''*-------- Création du fichier JSON métadonnées --------*'''

        meta = {'metadata': {
            'Source': link,
            'Nom du fichier': str(time.strftime('%Y-%m-%d_%H:%M:%S'))+'_'+nomFile+'.docx',
            'Date de création': '2019-10-22',
            'Date de modification': str(time.strftime('%Y-%m-%d %H:%M:%S')),
            'type de fichier': '.docx'
            }
        }
        return meta

    def insertData():

        '''*-------- DATA MATCH --------*'''

        link = 'http://www.footmercato.net/ligue-1/calendrier'
        source = BeautifulSoup(requests.get(link).text, 'lxml')

        dataBrut(source, 'Match ligue des champion')
        metadata = metaData(link,'Match ligue des champion')

        with open('/Users/romelus/Documents/Sites/StatFoot/metadata/'+str(time.strftime('%Y-%m-%d_%H:%M:%S'))+'-metadata_match.json', 'a', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=4)

        
        heures=[]
        for td in source.find_all('td', class_='wsmall'):
            heure = td.text
            heures.append(heure.replace('\n', ''))
            empt = list(filter(None,heures))
            if ' - ' in empt:
                empt.remove(' - ')

        print(empt)

        liste_team1=[]
        for td in source.find_all('td', class_='wlarge txtright bd-left'):
            equipe1 = td.text
            liste_team1.append(equipe1.replace('\n', ''))
        print(liste_team1)


        liste_team2=[]
        for td in source.find_all('td', class_='wlarge txtleft'):
            equipe2 = td.text
            liste_team2.append(equipe2.replace('\n', ''))
        print(liste_team2)

        dates=[]
        for td in source('td', class_='date imp'):
            date = td.text
            dates.append(date)
        print(dates)

        name_file= '/Users/romelus/Documents/Sites/StatFoot/fichier_csv/'+str(time.strftime('%Y-%m-%d_%H:%M:%S'))+'_'+'macths_ligue1.csv'
        with open(name_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["Heure","Equipe1", "Equipe2", "Date_match"])
            i=0
            for v1 in heures:
                v2 = liste_team1[i]
                v3 = liste_team2[i]
                v4 = dates[i]
                writer.writerow([v1,v2,v3,v4])
                i=i+1
        logging.debug('Sauvegarde fichier CSV match réussi !')

    insertData()

    def updateData():

        '''*-------- DATA TOP PLAYER --------*'''

        link = 'https://www.lequipe.fr/Football/classement-europeen-buteurs.html'
        source = BeautifulSoup(requests.get(link).text, 'lxml')
        
        dataBrut(source, 'top_player')
        metadata = metaData(link,'top_player')

        with open('/Users/romelus/Documents/Sites/StatFoot/metadata/'+str(time.strftime('%Y-%m-%d_%H:%M:%S'))+'-metadata_players.json', 'a', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=4)

        '''*-------- Rang --------*'''

        liste_rank=[]
        final_rand=[]
        for rang in source.find_all('td', class_='rand'):
            rank = rang.text
            liste_rank.append(rank)
            numb = len(liste_rank)
            if numb ==11:
                break
            final_rand.append(rank)
        #print(final_rand)

        '''*-------- Top 10 joueurs --------*'''

        liste_player=[]
        final_player=[]
        for td in source.find_all('strong'):
            player = td.text
            liste_player.append(player)
            numb = len(liste_player)
            if numb ==11:
                break
            final_player.append(player)
        #print(final_player)

        '''*-------- Nombre de buts --------*'''

        liste_buts=[]
        final_buts=[]
        for td in source.find_all('td', class_='but'):
            buts = td.text
            liste_buts.append(buts)
            numb = len(liste_buts)
            if numb ==11:
                break
            final_buts.append(buts)
        #print(final_buts)

        '''*-------- Nombre de match --------*'''

        liste_matchs=[]
        nb_matchs=[]
        for td in source.find_all('td', class_='match'):
            matchs = td.text
            liste_matchs.append(matchs)
            numb = len(liste_matchs)
            if numb ==11:
                break
            nb_matchs.append(matchs)
        #print(nb_matchs)
        logging.debug('Extraction des données des top joueurs réussi !')


        '''*-------- Creation du fichier CSV --------*'''

        name_file= '/Users/romelus/Documents/Sites/StatFoot/fichier_csv/'+str(time.strftime('%Y-%m-%d_%H:%M:%S'))+'_'+'top_player.csv'
        with open(name_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["Rang","Player", "But", "Nb_match"])
            i=0
            for v1 in final_player:
                v2 = final_buts[i]
                v3 = nb_matchs[i]
                v4 = final_rand[i]
                writer.writerow([v4,v1, v2, v3])
                i=i+1
        logging.debug('Sauvegarde fichier CSV top player réussi !')


        '''*-------- Lecture du fichier CSV et importation des données dans la BDD --------*'''
        df = pd.read_csv(name_file)

        list_rang=[]
        list_player=[]
        list_but=[]
        list_match=[]

        rank = df['Rang']
        player = df['Player']
        but = df['But']
        nb_match = df['Nb_match']


        #print(nb_match)
        i=0
        for v1 in player:
            players = v1
            buts = but[i]
            nb_matche = nb_match[i]
            rang = rank[i]
            list_player.append(players)
            list_but.append(buts)
            list_match.append(nb_matche)
            list_rang.append(rang)
            i+=1
        
        id_player=['1','2','3','4','5','6','7','8','9','10']

        i=0
        for v1 in list_player:
            curseur.execute("UPDATE top_player SET rang='%s', players='%s', buts='%s', nb_matchs='%s' WHERE id_player='%s' " %(list_rang[i],v1, list_but[i], list_match[i], id_player[i]))
            #conn.commit()
            i+=1
        logging.debug('Mise a jour des joueurs effectué avec success!')


        '''*-------- DATA TOP TEAM --------*'''
        
        link1 = 'https://www.les-sports.info/football-classement-mondial-des-clubs-s1-c2171-l0.html'
        source1 = BeautifulSoup(requests.get(link1).text, 'lxml')

        dataBrut(source1,'top_team')
        metadata2 = metaData(link1,'top_team')

        with open('/Users/romelus/Documents/Sites/StatFoot/metadata/'+str(time.strftime('%Y-%m-%d_%H:%M:%S'))+'-metadata_team.json', 'a', encoding='utf-8') as f:
            json.dump(metadata2, f, ensure_ascii=False, indent=4)

        list_rank=[]
        final_rank=[]
        for rang in source1.find_all('td', class_='tdcol-5'):
            rank = rang.text
            list_rank.append(rank)
            numb = len(list_rank)
            if numb == 11:
                break
            final_rank.append(rank)
        #print(final_rank)

        list_team=[]
        final_team=[]
        for rang in source1.find_all('a', class_='nodecort'):
            team = rang.text
            list_team.append(team)
            numb = len(list_team)
            if numb == 11:
                break
            final_team.append(team)
        #print(final_team)

        list_point=[]
        final_point=[]
        for rang in source1.find_all('td', class_='tdcol-15'):
            point = rang.text
            list_point.append(point)
            numb = len(list_point)
            if numb == 11:
                break
            final_point.append(point)
        #print(final_point)
        logging.debug('Extraction des données des top équipe réussi !')

        '''*-------- Creation du fichier CSV --------*'''

        name_file= '/Users/romelus/Documents/Sites/StatFoot/fichier_csv/'+str(time.strftime('%Y-%m-%d_%H:%M:%S'))+'_'+'top_team.csv'
        with open(name_file, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["Rang","Team", "Point"])
            i=0
            for v1 in final_rank:
                v2 = final_team[i]
                v3 = final_point[i]
                writer.writerow([v1, v2, v3])
                i=i+1
        logging.debug('Sauvegarde fichier CSV top team réussi !')

        '''*-------- Lecture du fichier CSV et importation des données dans la BDD --------*'''
        df = pd.read_csv(name_file)

        list_rang=[]
        list_team=[]
        list_point=[]

        rank = df['Rang']
        team = df['Team']
        point = df['Point']

        #print(nb_match)
        i=0
        for v1 in rank:
            teams = team[i]
            points = point[i]
            list_rang.append(v1)
            list_team.append(teams)
            list_point.append(points)
            i+=1
        
        id_team=['1','2','3','4','5','6','7','8','9','10']

        i=0
        for v1 in list_rang:
            curseur.execute("UPDATE top_team SET rang='%s', team='%s', point='%s' WHERE id_team='%s' " %(v1, list_team[i], list_point[i], id_team[i]))
            #conn.commit()
            i+=1
        logging.debug('Mise a jour des équipe effectué avec success!')

        print('Tous les traitements ont été réalisé avec success !')
    updateData()

traitements()

print("Execution - Traitement de nouvelle donnée\n")
s.enter(TIME, 1, traitements, (s,))
s.run()