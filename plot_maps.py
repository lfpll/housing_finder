import gmaps
from pymongo import MongoClient
from distance import distance
client = MongoClient()

coll = client['imoveis']['zap']
intrs= list(coll.find({'Area':{"$gte":50}},{'Latitude':1,'Longitude':1,'Valor':1,'PrecoCondominio':1,'UrlFicha':1}))
print(len(intrs))
aptos = []
gmaps.configure(api_key='AIzaSyAL0oq9pjtMT5tCgip8csNljuegC2abikg')
for apto in intrs:
	location = tuple((float(apto['Latitude']),float(apto['Longitude'])))
	if apto['PrecoCondominio'] and apto['PrecoCondominio'].find('R$') > -1:
		condominio = float(apto['PrecoCondominio'].split("R$")[1])
	else:
		condominio = float(apto['PrecoCondominio'])
	if apto['Valor'] and apto['Valor'].find('R$') > -1:
		valor = float(apto['Valor'].split("R$")[1].replace('.',''))
	else:
		valor = float(apto['Valor'])
	if(valor + condominio) <= 1500 and distance((-25.4344698,-49.2788269),location)<3:
		aptos.append({'price':valor+condominio,'location':location,'UrlFicha':apto['UrlFicha']})

plant_locations = [plant['location'] for plant in aptos]
info_box_template = """
<dl>
<dt>price</dt><dd>{price}</dd>
<a href="{UrlFicha}"<dt>UrlFicha</dt>/>
</dl>   
"""

plant_info = [info_box_template.format(**plant) for plant in aptos]
print('Dale')
marker_layer = gmaps.marker_layer(plant_locations,info_box_content=plant_info)
fig = gmaps.figure(center=(-25.4344698,-49.2788269))
fig.add_layer(marker_layer)
fig
