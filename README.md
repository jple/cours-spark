# cours-spark
## Contenu dossier script
<pre>
script/
|-- gen-rand
|   |-- gen-rand.sh
|   `-- stream.py
`-- gen-rand-csv
    |-- gen-rand-csv.sh
    `-- stream-csv.py
</pre>

## Utilisation des scripts
### gen-rand

**Contenu** :
- gen-rand.sh : genere des fichiers (nombre aleatoire) dans le dossier dump/
- stream.py : effectue le streaming du dossier dump/

**Utilisation** : 
Lancer deux terminaux vers la sandbox.  
- Dans l'un des terminaux, lancer le generateur :  
*script/gen-rand/gen-rand.sh*
- Dans l'autre, lancer le streaming :  
*python script/gen-rend/stream.py*



### gen-rand-csv
**Contenu** :
- gen-rand-csv.sh : genere des fichiers (nombre aleatoire) dans les dossiers dump/source1/ et dump/source2/
- stream-csv.py : effectue le streaming des dossiers

Attention : il faut au préalable créer les dossiers !

**Utilisation** : 
Lancer deux terminaux vers la sandbox.  
- Dans l'un des terminaux, lancer le generateur :  
*script/gen-rand-csv/gen-rand.-csvsh*
- Dans l'autre, lancer le streaming :  
*python script/gen-rend-csv/stream-csv.py*
