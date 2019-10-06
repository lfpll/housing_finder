import os
import re
from datetime import datetime
import json
import base64
from bs4 import BeautifulSoup
from google.cloud import storage, error_reporting
import unidecode

# Regular expressions used on the parsing

REGEXP_PRICE = re.compile(
    r'^(\w+)(?:\s*)R\$(?:\s*)(\w+\.?(?:\w+)?\,?(?:\w+)?)')
REGEXP_MARKERS = re.compile(r'markers=(.+?)\&')
REGEXP_CORDINATES = re.compile('\'mapLat\'|\'mapLng\'')

# Enviroment variables
_IN_BUCKET = os.environ['IN_BUCKET']  # 'imoveis-data'
_OUT_BUCKET = os.environ['OUT_BUCKET']  # bigtable-data


def parse_propertie_page(data, context):
    """This a lambda function for cloud function on the google cloud
       This receiveis two inputs 
       The interesting one is the data which comes from pub/sub
       This get a path an html from gcs insite data['data']['filename'] and parse this html
       At the end with this html parsed it's store on the _OUT_BUCKET as a json

    Arguments:
        data {[dict]} -- [{"file_name":GCS path of file, "url":the url origin of the file}]
        context {[dict]} -- [description]

    Raises:
        Exception: [Exception for erros on the process]
    """
    try:
        # Initializing the data
        error_client = error_reporting.Client()
        client = storage.Client()
        bucket = client.get_bucket(_IN_BUCKET)
        json_obj = json.loads(base64.b64decode(data['data']).decode('utf-8'))

        file_path = json_obj['file_path']
        url = json_obj['url']
        blob = bucket.blob(file_path)
        html_data = blob.download_as_string()
        soup = BeautifulSoup(html_data, 'lxml')

        # CSS paths of the data information
        price_block = soup.select('div.block-price-container')
        attrs_block = soup.select('ul.section-icon-features')
        addts_block = soup.select('ul.section-bullets')
        local_block = soup.select('div.article-map')
        pub_code = list(
            set(soup.find_all('span', {'class': 'publisher-code'})))
        pub_date = soup.find('h5', {'class': ['section-date', 'css-float-r']})

        scripts = soup.find_all('script')
        filter_scripts = list(
            filter(lambda val: REGEXP_CORDINATES.search(val.text), scripts))

        # Transforming data indo a format of interest
        # List of tuples to transformed into json
        final_tups = []

        # Getting the description of the properties
        description = soup.find('div', id='verDatosDescripcion')
        if description is not None:
            final_tups.append(('descricao', description.text.strip()))

        # Urls of the imgs
        try:
            img_urls = []
            for img in soup.find('div', id='tab-foto-flickity').find_all('img'):
                attrs = img.attrs
                if 'src' in attrs and attrs['src'].startswith('http'):
                    img_urls.append(attrs['src'])
                elif 'alt' in attrs and attrs['alt'].startswith('http'):
                    img_urls.append(attrs['alt'])
                elif 'lazyload' in attrs and attrs['lazyload'].startswith('http'):
                    img_urls.append(attrs['lazyload'])
                elif 'data-flickity-lazyload' in attrs and \
                        attrs['data-flickity-lazyload'].startswith('http'):
                    img_urls.append(attrs['data-flickity-lazyload'])
            final_tups.append(('imgs', img_urls))
        except:
            error_client.report_exception()
        final_tups.append(('url', url))

        # Find title and information in it lie adress and neighborhood
        title_address = soup.find('h2', {'class': 'title-location'})
        if title_address is not None:
            address = title_address.find('b')
            neighborhood = title_address.find('span')
            if address is not None:
                final_tups.append(('endereco', address.text.strip()))
            if neighborhood is not None:
                final_tups.append(('bairro', neighborhood.text.strip()))

        # Additions of blocks, specific information about apartment
        if addts_block:
            audits_final_list = []
            addits_list = [additives.find_all('li')
                           for additives in addts_block]
            for additive in addits_list:
                audits_final_list.extend(additive)
            audits_final_list = [unidecode.unidecode(
                auditive.text.strip()) for auditive in audits_final_list]
            final_tups.append(('additions', audits_final_list))

        # Dynamical attributes gotten from page
        if len(attrs_block) == 1:
            attrs_list = attrs_block[0].select('li')
            attrs_list = [(attrs.find('span').text.strip(),
                           unidecode.unidecode(attrs.find('b').text.strip())) for attrs in
                          attrs_list]
            final_tups.extend(attrs_list)

        # Get dynamical information about the prices
        if len(price_block) == 1:
            price_list = price_block[0].text.strip().split('\n')
            price_list = [REGEXP_PRICE.search(
                price) for price in price_list if REGEXP_PRICE.search(price)]
            price_list = [(price_regexp.group(1), float(price_regexp.group(2).replace('.', '').replace(',', '.'))) for
                          price_regexp in price_list]
            final_tups.extend(price_list)

        # Block of latitude and longitude
        if len(local_block) == 1 or filter_scripts:
            lat_long = False
            if filter_scripts:
                lat_long_script = filter_scripts[0].text
                lat_long = list(filter(lambda val: REGEXP_CORDINATES.search(
                    val), lat_long_script.split('\n')))
                lat_long = list(map(lambda val: tuple(
                    val.replace(' ', '').replace("'", '').replace(',', '').
                    replace('mapLat', 'latitude').
                    replace('mapLng', 'longitude').strip().split(
                        ':')), lat_long))
                lat_long = [tuple((key, float(val))) for key, val in lat_long]
            else:
                image_url = local_block[0].find('img').text
                if REGEXP_MARKERS.search(image_url):
                    url_parse = REGEXP_MARKERS.search(
                        image_url['src']).group(1).split(',')
                    lat_long = [float(float_val) for float_val in url_parse]
                    lat_long = [('latitude', float(lat_long[0].replace(',', ''))), float(
                        'longitute', lat_long[1].replace(',', ''))]
            if lat_long:
                final_tups.extend(lat_long)

        # Publisher info
        if pub_code:
            # Getting the code of the apartament and the anouncer code
            for soup_obj in pub_code:
                text = soup_obj.text.split(':')
                if text[0].find('anunciante') > -1:
                    final_tups.append(('pub_anun', text[-1].strip()))
                elif text[0].find('Imovelweb') > -1:
                    final_tups.append(('pub_code', int(text[-1].strip())))

        # Publication Date
        if pub_date is not None:
            final_tups.append(('pub_data', pub_date.text.strip()))

        # No data gotten from value
        if not final_tups:
            raise ValueError('Impossible do parse %s' % file_path)

        # Adding current date for bigquery
        final_tups.append(('date_stored', str(datetime.now())))

        json_file = json.dumps({unidecode.unidecode(key).strip().replace(
            ' ', '_').lower(): val for key, val in final_tups})
        
        bucket = client.get_bucket(_OUT_BUCKET)
        folder = 'stage'
        if not json_obj['new_blob']: 
            folder = 'update_stage'
        new_blob = bucket.blob(
                '{0}/{1}.json'.format(folder,file_path.replace('.html', '')))
        new_blob.upload_from_string(json_file)

    except Exception:
        error_client = error_reporting.Client()
        error_client.report_exception()
