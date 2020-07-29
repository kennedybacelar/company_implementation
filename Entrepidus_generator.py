import pandas as pd
pd.options.mode.chained_assignment = None
import numpy as np
import sys
from datetime import datetime, date
sys.path.insert(1, 'Ent_generator')
import logger
import os
import warnings

warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)

def getting_user_input():

    print('*** Save your store.txt file in UTF-8 format ***')

    STR_indicator = False

    root_path = input('Please inform root path: \n')
    root_path = root_path.replace('\\', '/')

    country = input('Please inform the country of the distrbutor: \n')
    country = country.lower()

    STR_country_list = ['paraguay', 'uruguay']

    if (country in STR_country_list):
        STR_indicator = True

    return [root_path, country, STR_indicator]

def getting_system_paths(root_path, country, STR_indicator):

    sales_file_path = str(root_path) + '/sales.txt'
    store_txt_file_path = root_path + '/store.txt'

    catalogs_root_path = '../../../Catalogs/Traditional_STR/'
    product_by_distributor_file_name = 'pebac_ref_prod.xlsx'

    if STR_indicator:
        product_by_distributor_file_name = 'str_ref_prod.xlsx'

    pebac_master_data_product_file_path = catalogs_root_path + 'Product_catalog/' + product_by_distributor_file_name
    product_master_path = catalogs_root_path + 'Product_catalog/product_master.xlsx'
    customer_catalog_file_path = catalogs_root_path + 'Customer_catalog/' + country + '_customer_catalog.xlsx'
    dist_names_file_path = catalogs_root_path + 'dist_names.xlsx'
    customer_filling_reference_file_path = catalogs_root_path + 'Customer_catalog/z_customer_reference.xlsx'

    entrepidus_stock_directory_path = '/'.join(root_path.split('/')[:-1])
    entrepidus_stock_file_path = entrepidus_stock_directory_path + '/Entrepidus_STOCK.csv'

    system_paths = [sales_file_path, pebac_master_data_product_file_path, 
            product_master_path, customer_catalog_file_path, dist_names_file_path, root_path,
            entrepidus_stock_file_path, store_txt_file_path, customer_filling_reference_file_path]

    return system_paths

def loading_dataframes(system_paths):

    sales_file_path = system_paths[0]
    pebac_master_data_product_file_path = system_paths[1]
    product_master_path = system_paths[2]
    customer_catalog_file_path = system_paths[3]
    dist_names_file_path = system_paths[4]

    df_sales_columns = ['Country', 'Diageo Customer ID', 'Diageo Customer Name', 
    'Invoice number', 'Type of Invoice',	'Invoice Date', 'Store code', 'Product Code', 
    'Quantity', 'Unit of measure', 'Total Amount WITHOUT TAX', 'Total Amount WITH TAX', 
    'Currency Code', 'Sales Representative Code']

    #Loading Data Frame of Sales File
    try:
        df_sales = pd.read_csv(sales_file_path, index_col=False, names=df_sales_columns,sep=';', low_memory=False,
        dtype={ 'Quantity':str, 'Store code':str, 'Product Code':str, 'Invoice Date':str,
        'Total Amount WITH TAX':str, 'Total Amount WITHOUT TAX':str  }, header=0).fillna('')
    except:
        logger.logger.error('Not possible opening the file {}'.format(sales_file_path))
        print('Not possible opening the file - {}'.format(sales_file_path))
        sys.exit()

    #Loading Data Frame of (De->Para) / Product Customer -> Diageo SKU
    try:
        df_pebac_product_reference = pd.read_excel(pebac_master_data_product_file_path, converters = { 'Dist_Code': str, 'Product_store_id': str} ).fillna('')
        df_pebac_product_reference.set_index(['Dist_Code', 'Product_store_id'], inplace=True)        
    except:
        logger.logger.info('Not possible opening the file / setting index {}'.format(pebac_master_data_product_file_path))
        print('Not possible opening the file - {}'.format(pebac_master_data_product_file_path))
        sys.exit()

    #Loading Data Frame of Product Master Data
    try:
        df_product_master = pd.read_excel(product_master_path, dtype={ 'Material': str }).fillna('')      
    except:
        logger.logger.info('Not possible opening the file {}'.format(product_master_path))
        print('Not possible opening the file - {}'.format(product_master_path))
        sys.exit()

    #Loading Data Frame of Customer Catalog Per Country
    try:
        df_customer_catalog = pd.read_excel(customer_catalog_file_path, converters={ 'Distributor_id':str, 'Store_id':str } ).fillna('')       
    except:
        logger.logger.info('Not possible opening the file {}'.format(customer_catalog_file_path))
        print('Not possible opening the file - {}'.format(customer_catalog_file_path))
        sys.exit()
    
    #Loading Data Frame of Distributors correct name and country
    try:
        df_dist_names = pd.read_excel(dist_names_file_path, dtype=str ).fillna('')
    except:
        print('Not possible opening file - {}'.format(dist_names_file_path))
        logger.logger.error('Not possible opening file - {}'.format(dist_names_file_path))
        sys.exit()


    #Dropping unecessary columns of Dataframes to keep processing light
    try:
        df_sales.drop(columns=['Type of Invoice', 'Sales Representative Code'], inplace=True)
    except:
        logger.logger.info('Not possible dropping Columns of file - {}'.format(sales_file_path))

    #Dropping unecessary columns of Product_master field
    try:
        df_product_master.drop(columns=['PRDHA L7 Packaging', 'Packaging', 'PRDHA L6 Volume',
            'Subbrand', 'PRDHA L4 Brand Variant', 'PRDHA L3 Brand', 'PRDHA L2 Group',
            'Group', 'PRDHA L1 Main Group', 'EU Size', 'Case Size'], inplace=True)
    except:
        logger.logger.info('Not possible dropping Columns of file - {}'.format(product_master_path))

    return [df_sales, df_pebac_product_reference, df_product_master, df_customer_catalog, df_dist_names]


def sanitizing_sales_file(df_sales):
    
    #Removing negative sign from the end of the values (Some samples were found)
    values_that_end_with_negative_sign_quantity = (df_sales['Quantity'].str[-1] == '-')
    df_sales.loc[values_that_end_with_negative_sign_quantity, 'Quantity'] = '-' + df_sales.loc[values_that_end_with_negative_sign_quantity, 'Quantity'].str[:-1]
    
    values_that_end_with_negative_sign_total_with_tax = (df_sales['Total Amount WITH TAX'].str[-1] == '-')
    df_sales.loc[values_that_end_with_negative_sign_total_with_tax, 'Total Amount WITH TAX'] = '-' + df_sales.loc[values_that_end_with_negative_sign_total_with_tax, 'Total Amount WITH TAX'].str[:-1]
    
    values_that_end_with_negative_sign_total_without_tax = (df_sales['Total Amount WITHOUT TAX'].str[-1] == '-')
    df_sales.loc[values_that_end_with_negative_sign_total_without_tax, 'Total Amount WITHOUT TAX'] = '-' + df_sales.loc[values_that_end_with_negative_sign_total_without_tax, 'Total Amount WITHOUT TAX'].str[:-1]
    
    #Turning it numeric below quantities
    df_sales['Quantity'] = pd.to_numeric(df_sales['Quantity']).fillna(0)
    df_sales['Total Amount WITH TAX'] = pd.to_numeric(df_sales['Total Amount WITH TAX']).fillna(0)
    df_sales['Total Amount WITHOUT TAX'] = pd.to_numeric(df_sales['Total Amount WITHOUT TAX']).fillna(0)
    
    #Removing spaces and leading zeros from below columns
    df_sales['Product Code'] = df_sales['Product Code'].str.lstrip('0')
    df_sales['Store code'] = df_sales['Store code'].str.lstrip('0')
    df_sales['Store code'] = df_sales['Store code'].str.strip()

    #Cutting characters after the 12th position from Store Code column
    df_sales['Store code'] = df_sales['Store code'].str[:12]

    return df_sales

def sanitizing_df_pebac_product_reference(df_pebac_product_reference):

    df_pebac_product_reference.columns = [column.encode('mbcs').decode('mbcs', 'ignore') for column in df_pebac_product_reference.columns]
    df_pebac_product_reference['Scale'] = pd.to_numeric(df_pebac_product_reference['Scale']).fillna(1)

    return df_pebac_product_reference

def declaring_entrepidus_df():

    entrepidus_columns = ['Date', 'Store Number', 'Store Name', 'Chain', 'Supervisor', 'Region',
        'Commune', 'Merchandiser', 'Chain SKU Code', 'Diageo SKU Code',	'Desc Producto & Cód.',
        'Category', 'Sub Category', 'Brand', 'Brand Variant', 'Unit Size', 'Unit Sold', 
        'Sales Value wotax', 'Sales Value wtax', 'Currency Code', 'Distributor', 'Country', 
        'Inventory Unit', 'Diageo_dist_auxiliar_column']

    try:
        try:
            df_entrepidus = pd.DataFrame(columns=entrepidus_columns).fillna('')
        except IOError as err:
            logger.logger.info('{}'.format(err))
            sys.exit(err)
    except:
        logger.logger.info('Not possible creating DataFrame df_entrepidus')
        sys.exit('Not possible creating DataFrame df_entrepidus')
    
    return df_entrepidus

def setting_df_entrepidus_and_sales(df_entrepidus, df_sales):

    try:
        df_entrepidus['Country'] = df_sales['Country']
        df_entrepidus['Sales Value wotax'] = df_sales['Total Amount WITHOUT TAX']
        df_entrepidus['Sales Value wtax'] = df_sales['Total Amount WITH TAX']
        df_entrepidus['Currency Code'] = df_sales['Currency Code']
        df_entrepidus['Store Number'] = df_sales['Store code']
        df_entrepidus['Date'] = df_sales['Invoice Date']
        df_entrepidus['Chain SKU Code'] = df_sales['Product Code']
        df_entrepidus['Distributor'] = df_sales['Diageo Customer Name']
        df_entrepidus['Unit Sold'] = df_sales['Quantity']
        df_entrepidus['Inventory Unit'] = 0

        #Auxiliar Columns - Won't be written into the excel file
        df_entrepidus['Diageo_dist_auxiliar_column'] = df_sales['Diageo Customer ID']
        df_entrepidus['Aux_unit_of_measure'] = df_sales['Unit of measure']

        #Changing to String below Columns
        df_entrepidus['Diageo_dist_auxiliar_column'] = df_entrepidus['Diageo_dist_auxiliar_column'].astype(str).fillna('')
        df_sales['Product Code'] = df_sales['Product Code'].astype(str).fillna('')
        df_entrepidus['Store Number'] = df_entrepidus['Store Number'].astype(str).fillna('')
        #Changing to Numeric below Columns
        df_entrepidus['Unit Sold'] = pd.to_numeric(df_entrepidus['Unit Sold'])
        #Lowering entrepidus series
        df_entrepidus['Aux_unit_of_measure'] = df_entrepidus['Aux_unit_of_measure'].astype(str).fillna('').str.lower()
    except:
        logger.logger.error('Not possible setting_df_entrepidus / sales')
        sys.exit('Not possible setting_df_entrepidus')
    
    return [df_entrepidus, df_sales]

def assigning_dist_names_and_country_to_entrepidus(df_entrepidus, df_dist_names):

    list_of_distributors = df_entrepidus['Diageo_dist_auxiliar_column'].unique()
    df_entrepidus.set_index(['Diageo_dist_auxiliar_column'], inplace=True)
    df_entrepidus.index = df_entrepidus.index.map(str)

    df_dist_names.set_index(['Distributor_id'], inplace=True)
    df_dist_names.index = df_dist_names.index.map(str)
    df_dist_names = df_dist_names[~df_dist_names.index.duplicated(keep='first')]

    for single_distributor in list_of_distributors:
        single_distributor = str(single_distributor)

        try:
            distributor_correct_name = df_dist_names.loc[single_distributor, 'Distributor_name']
            distributor_correct_country = df_dist_names.loc[single_distributor, 'Distributor_country']
        except:
            print('Dist name columns Distributor_name or Distributor_country not found')
            logger.logger.error('Dist name columns Distributor_name or Distributor_country not found')

        try:
            df_entrepidus.loc[single_distributor, 'Distributor'] = distributor_correct_name
        except:
            print('Error- Distributor name in dist_names file: {}'.format(single_distributor))
            logger.logger.error('Not possible assigning distributor name from Dist_names_file - {}'.format(single_distributor))
        
        try:
            df_entrepidus.loc[single_distributor, 'Country'] = distributor_correct_country
        except:
            print('Not possible assigning distributor country from Dist_names_file - {}'.format(single_distributor))
            logger.logger.error('Not possible assigning distributor country from Dist_names_file - {}'.format(single_distributor))
        
    df_dist_names.reset_index(inplace=True)    
    df_entrepidus.reset_index(inplace=True)
    return df_entrepidus

def searching_diageo_sku(df_sales, df_product_master, df_entrepidus):

    list_of_distributors = df_sales['Diageo Customer ID'].unique()
    
    df_sales = df_sales.set_index(['Diageo Customer ID'])
    df_sales.index = df_sales.index.map(str)

    df_entrepidus = df_entrepidus.set_index(['Diageo_dist_auxiliar_column', 'Chain SKU Code'])
    df_entrepidus.index = df_entrepidus.index.set_levels(df_entrepidus.index.levels[0].astype(str), level=0)
    df_entrepidus.index = df_entrepidus.index.set_levels(df_entrepidus.index.levels[1].astype(str), level=1)
        
    for single_distributor in list_of_distributors:
        single_distributor  = str(single_distributor)
        products_list_by_distributor = df_sales.loc[single_distributor]['Product Code'].unique()

        for single_product_by_distributor in products_list_by_distributor:
            single_product_by_distributor = str(single_product_by_distributor)

            try:
                diageo_sku = df_product_master.loc[(single_distributor, single_product_by_distributor), 'Diageo_Sku'].values[0]
                df_entrepidus.loc[(single_distributor, single_product_by_distributor), 'Diageo SKU Code'] = diageo_sku
            except:
                df_entrepidus.loc[(single_distributor, single_product_by_distributor), 'Diageo SKU Code'] = '0000 - NOT FOUND'
                print('{} - New product found'.format(single_product_by_distributor))
                logger.logger.warning('{} - Product not found'.format(single_product_by_distributor))

    df_entrepidus.reset_index(inplace = True)
    df_product_master.reset_index(inplace=True)
    return df_entrepidus
    

#Filling Entrepidus with the product details
def filling_product_details(df_entrepidus, df_product_master):

    df_product_master.set_index(['Material'], inplace=True)
    df_product_master.index = df_product_master.index.map(str) #Changing indexes into string
    df_product_master = df_product_master[~df_product_master.index.duplicated(keep='last')]

    list_of_diageo_sku_unique = df_entrepidus['Diageo SKU Code'].unique()

    df_entrepidus.set_index(['Diageo SKU Code'], inplace=True)
    df_entrepidus.index = df_entrepidus.index.map(str) #Changing indexes into string

    for specific_diageo_sku in list_of_diageo_sku_unique:
        specific_diageo_sku = str(specific_diageo_sku)
        try:
            df_entrepidus['Desc Producto & Cód.'].loc[specific_diageo_sku] = df_product_master['Description'].loc[specific_diageo_sku]
            df_entrepidus['Category'].loc[specific_diageo_sku] = df_product_master['Main Group'].loc[specific_diageo_sku]
            df_entrepidus['Sub Category'].loc[specific_diageo_sku] = df_product_master['Subcategory'].loc[specific_diageo_sku]
            df_entrepidus['Brand'].loc[specific_diageo_sku] = df_product_master['Brand'].loc[specific_diageo_sku]
            df_entrepidus['Brand Variant'].loc[specific_diageo_sku] = df_product_master['Brand Variant'].loc[specific_diageo_sku]
            df_entrepidus['Unit Size'].loc[specific_diageo_sku] = df_product_master['Unit Size'].loc[specific_diageo_sku]
        except:
            logger.logger.error('{} - Not possible filling this product details'.format(specific_diageo_sku))
        
    df_entrepidus.reset_index(inplace=True)
    return df_entrepidus


#Filling Entrepidus with quantities (Unit sold - after multiplying for the product tx)
def calculating_quantity(df_entrepidus, df_pebac_product_reference):

    df_pebac_product_reference.set_index(['Dist_Code', 'Product_store_id'], inplace=True)
    #Changing the first level of a multindex to String
    df_pebac_product_reference.index = df_pebac_product_reference.index.set_levels(df_pebac_product_reference.index.levels[0].astype(str), level=0)
    df_pebac_product_reference.index = df_pebac_product_reference.index.set_levels(df_pebac_product_reference.index.levels[1].astype(str), level=1)

    list_of_distributors =  df_entrepidus['Diageo_dist_auxiliar_column'].unique()

    for single_distributor in list_of_distributors:
        single_distributor = str(single_distributor)

        filt_single_distributor = (df_entrepidus['Diageo_dist_auxiliar_column'] == single_distributor)
        list_of_products_by_distributor = df_entrepidus.loc[filt_single_distributor, 'Chain SKU Code'].unique()

        df_entrepidus['Diageo_dist_auxiliar_column'] = df_entrepidus['Diageo_dist_auxiliar_column'].astype(str).fillna('')
        df_entrepidus['Chain SKU Code'] = df_entrepidus['Chain SKU Code'].astype(str).fillna('')

        for single_product in list_of_products_by_distributor:
            single_product = str(single_product)

            try:
                multiplicative_factor = df_pebac_product_reference.loc[( single_distributor , single_product ), 'Scale'].values
                multiplicative_factor = multiplicative_factor[0]
            except:
                logger.logger.info('multiplicative_factor not found in df_pebac_product_reference for Distributor - {} Product - {}'.format(single_distributor, single_product))
                multiplicative_factor = 1

            try:
                filt_key_dist_prod = (df_entrepidus['Diageo_dist_auxiliar_column'] == str(single_distributor) ) & (df_entrepidus['Chain SKU Code'] == str(single_product)) 
                df_entrepidus.loc[filt_key_dist_prod, 'Unit Sold'] = df_entrepidus.loc[filt_key_dist_prod, 'Unit Sold'].multiply(multiplicative_factor)
            except:
                logger.logger.error(' Error multiplication - Bottles por Physical Case - dist/product {}/{}'.format(single_distributor, single_product))
            
    try:
        df_entrepidus['Unit Sold'] = df_entrepidus['Unit Sold'].round(0).astype(int)
    except:
        logger.logger.error('Not possible rounding df_entrepidus[Unit Sold]')

    df_pebac_product_reference.reset_index(inplace=True)
    df_entrepidus.reset_index(inplace=True)

    return df_entrepidus

#Filling Entrepidus with the store names
def getting_store_name(df_entrepidus, df_customer_catalog):

    new_stores = list()

    df_customer_catalog.set_index([ 'Distributor_id', 'Store_id' ], inplace=True)
    #Changing the first level of a multindex to String
    df_customer_catalog.index = df_customer_catalog.index.set_levels(df_customer_catalog.index.levels[0].astype(str), level=0)
    df_customer_catalog.index = df_customer_catalog.index.set_levels(df_customer_catalog.index.levels[1].astype(str), level=1)
    
    list_of_distributors =  df_entrepidus['Diageo_dist_auxiliar_column'].unique()

    df_entrepidus['Diageo_dist_auxiliar_column'] = df_entrepidus['Diageo_dist_auxiliar_column'].astype(str).fillna('')
    df_entrepidus['Store Number'] = df_entrepidus['Store Number'].astype(str).fillna('')

    for single_distributor in list_of_distributors:
        single_distributor = str(single_distributor)
        
        filt_single_distributor = (df_entrepidus['Diageo_dist_auxiliar_column'] == single_distributor)
        list_of_unique_stores_by_distributor = df_entrepidus.loc[(filt_single_distributor), 'Store Number'].unique()

        
        for unique_store in list_of_unique_stores_by_distributor:
            unique_store = str(unique_store)

            try:
                store_name = df_customer_catalog.loc[[(single_distributor, unique_store)], 'Store_name'].values
                store_name = store_name[0]
            except:
                new_unique_store = single_distributor + '|' + unique_store
                new_stores.append(new_unique_store)
                store_name = '0000 - NOT FOUND'

            try:
                filt_single_store_by_distributor = ((df_entrepidus['Diageo_dist_auxiliar_column'] == str(single_distributor)) & (df_entrepidus['Store Number'] == str(unique_store)))
                df_entrepidus.loc[(filt_single_store_by_distributor), 'Store Name'] = store_name
            except:
                pass

    df_customer_catalog.reset_index(inplace=True)

    return [df_entrepidus, new_stores]


#Filtering Period - Unused yet
def filtering_period(df_entrepidus, previous_and_current_month_period):

    current_month = previous_and_current_month_period[0]
    previous_month = previous_and_current_month_period[1]

    entrepidus_filtered_period = ((df_entrepidus['Date'].str[:6] == current_month) | (df_entrepidus['Date'].str[:6] == previous_month))
    df_entrepidus = df_entrepidus.loc[entrepidus_filtered_period]

    return df_entrepidus

def creating_new_stores_dataframe():

    new_store_columns = ['Aux_column_dist_number', 'POS_ID', 'Store Nbr', 'Store Name', 'Chain', 'Commercial Group', 'Store/Business Type',
    'Subchannel', 'Channel', 'Trade', 'Segment', 'Occasion', 'Occasion Segment', 'Mechandiser', 'Supervisor',
    'Provice or Commune', 'City', 'State or Region', 'Country', 'COU']

    df_new_stores = pd.DataFrame(columns=new_store_columns).fillna('')
    
    return df_new_stores

# Registering new stores
def registering_new_stores(new_stores, df_new_stores):

    unique_stores = list(set(new_stores)) #Getting new stores - Filtering and getting unique values

    for individual_store in unique_stores:

        distributor_and_store_split = individual_store.split('|')
        distributor_id = distributor_and_store_split[0]
        store_number = distributor_and_store_split[1]

        df_new_stores_lenght = len(df_new_stores)

        df_new_stores.loc[df_new_stores_lenght, 'Aux_column_dist_number'] = distributor_id
        df_new_stores.loc[df_new_stores_lenght, 'Store Nbr'] = store_number
    
    df_new_stores.fillna('', inplace=True)

    return df_new_stores


def loading_store_txt_file_and_customer_filling_reference(
    store_txt_file_path, 
    STR_indicator,
    customer_filling_reference_file_path):

    if STR_indicator:
        store_header = 1
    else:
        store_header = None

    df_store_txt_file_columns = [ 'Diageo Customer ID', 'Diageo Customer Name', 'Store Code',
    'Store Name','City','Region', 'Sales Representative Code',
    'Sales Representative Name', 'Local Segment 1','Local Segment 2', 'Local Segment 3', 'Local Segment 4']

    try:
        df_store_txt_flat_file = pd.read_csv(store_txt_file_path, encoding='utf-8',
            names=df_store_txt_file_columns, sep=';', low_memory=False,
            dtype=str, header=store_header).fillna('')
    except:
        logger.logger.error('Not possible loading df_store_txt_flat_file')
        print('Not possible loading df_store_txt_flat_file')
    
    try:
        df_z_customer_standard_filling_reference = pd.read_excel(customer_filling_reference_file_path, 
        dtype=str).fillna('')
    except:
        logger.logger.error('Not possible loading customer_filling_reference_file_path')
        print('Not possible loading customer_filling_reference_file_path')

    return [df_store_txt_flat_file, df_z_customer_standard_filling_reference]


def declaring_dictionaries():

    dict_store_vs_customer_catalog_A = {
        'Chain': 'Diageo Customer Name',
        'Store Nbr': 'Store Code',
        'Store Name': 'Store Name',
        'City': 'City',
        'State or Region': 'Region',
        'Occasion Segment': 'Sales Representative Code',
        'Occasion': 'Sales Representative Name',
        'Store/Business Type': 'Local Segment 2',
        'Channel': 'Local Segment 1',
        'Trade': 'Local Segment 2',
        'Subchannel': 'Local Segment 3',
        'Segment': 'Local Segment 4'
    }

    dict_store_vs_customer_catalog_B = {
        'Chain': 'Diageo Customer Name',
        'Store Nbr': 'Store Code',
        'Store Name': 'Store Name',
        'City': 'City',
        'State or Region': 'Region',
        'Occasion Segment': 'Sales Representative Code',
        'Occasion': 'Sales Representative Name',
        'Store/Business Type': 'Local Segment 2',
        'Channel': 'Local Segment 1',
        'Trade': 'Local Segment 1',
        'Subchannel': 'Local Segment 3',
        'Segment': 'Local Segment 2'
    }

    return [dict_store_vs_customer_catalog_A, dict_store_vs_customer_catalog_B]


def filling_new_stores_details(
        df_new_stores,
        df_store_txt_flat_file,
        df_z_customer_standard_filling_reference,
        dict_store_vs_customer_catalog_A,
        dict_store_vs_customer_catalog_B
    ):

    df_z_customer_standard_filling_reference.set_index(['Dist_id_auxiliar'], inplace=True)
    df_z_customer_standard_filling_reference.index = df_z_customer_standard_filling_reference.index.map(str)

    df_store_txt_flat_file.set_index([ 'Diageo Customer ID', 'Store Code' ], inplace=True)
    df_store_txt_flat_file.index = df_store_txt_flat_file.index.set_levels(df_store_txt_flat_file.index.levels[0].astype(str), level=0)
    df_store_txt_flat_file.index = df_store_txt_flat_file.index.set_levels(df_store_txt_flat_file.index.levels[1].astype(str), level=1)

    df_store_txt_flat_file = df_store_txt_flat_file[~df_store_txt_flat_file.index.duplicated(keep='last')]

    columns_to_be_iterated = df_z_customer_standard_filling_reference.columns[3:]

    for index in df_new_stores.index:

        distributor = str(df_new_stores.loc[index, 'Aux_column_dist_number'])
        store_code = str(df_new_stores.loc[index, 'Store Nbr'])

        try:
            dictionary_version = df_z_customer_standard_filling_reference.loc[distributor, 'Dictionary_version']
        except:
            print('Dictionary_version not found')
            logger.logger.error('Dictionary_version not found')
            sys.exit()
        
        if (dictionary_version == 'B'):
            dict_store_vs_customer_catalog = dict_store_vs_customer_catalog_B
        else:
            dict_store_vs_customer_catalog = dict_store_vs_customer_catalog_A

        for column_of_df_new_stores in columns_to_be_iterated:

            if column_of_df_new_stores in dict_store_vs_customer_catalog:
                column_df_store_txt_flat_file = dict_store_vs_customer_catalog[column_of_df_new_stores]

            try:
                result = df_z_customer_standard_filling_reference.loc[distributor, column_of_df_new_stores]
            except:
                print('Not possible finding column - {} - in df_z_customer_standard_filling_reference File'.format(column_of_df_new_stores))


            if(result == 'N'):
                try:
                    df_new_stores.loc[index, column_of_df_new_stores] = df_store_txt_flat_file.loc[(distributor, store_code), column_df_store_txt_flat_file]
                except:
                    print('Not possible assigning Dist - {} and Store - {} from Store.txt file. Columns {} -> {}'.format(distributor,
                    store_code, column_of_df_new_stores, column_df_store_txt_flat_file))
            else:
                try:
                    df_new_stores.loc[index, column_of_df_new_stores] = result
                except:
                    print('Error when trying to assign from reference_customer file')
            
            try:
                df_new_stores['Trade'] = df_new_stores['Trade'].astype(str)
                df_new_stores['Store Name'] = df_new_stores['Store Name'].astype(str)
                
                df_new_stores['Trade'] = df_new_stores['Trade'].str[:15]
                df_new_stores['Store Name'] = df_new_stores['Store Name'].str[:100]
            except Exception as error:
                print(error)

    return df_new_stores


def sanitizing_df_store_txt_flat_file(df_store_txt_flat_file):

    df_store_txt_flat_file['Store Code'] = df_store_txt_flat_file['Store Code'].str.lstrip('0')
    df_store_txt_flat_file['Store Code'] = df_store_txt_flat_file['Store Code'].str.strip()
    df_store_txt_flat_file['Store Code'] = df_store_txt_flat_file['Store Code'].str[:12]

    return df_store_txt_flat_file


# Getting current and previous month
def get_previous_and_current_month_period():
    
    today = date.today()
    month = today.month
    year = today.year

    if (month == 1):
        year_previous_month = year - 1
        previous_month = 12
    else:
        year_previous_month = year
        previous_month = month - 1
    
    current_month = str(year) + str(month).zfill(2)
    previous_month = str(year_previous_month) + str(previous_month).zfill(2)

    return [current_month, previous_month]

#Final formatting entrepidus
def entrepidus_formatting(df_entrepidus):

    try:
        df_entrepidus['Store Name'] = df_entrepidus['Store Name'].astype(str)
        df_entrepidus['Store Name'] = df_entrepidus['Store Name'].str[:100]
    except:
        print('Not possible cutting store name field from Entrepidus')

    df_entrepidus.reset_index(inplace=True)
    try:
        df_entrepidus.drop(columns=['level_0', 'index'], inplace=True)
    except:
        logger.logger.warning('Not possible dropping columns to generate excel file')

    entrepidus_columns = ['Diageo_dist_auxiliar_column', 'Date', 'Store Number', 'Store Name', 'Chain', 'Supervisor', 'Region',
        'Commune', 'Merchandiser', 'Chain SKU Code', 'Diageo SKU Code',	'Desc Producto & Cód.',
        'Category', 'Sub Category', 'Brand', 'Brand Variant', 'Unit Size', 'Unit Sold', 
        'Sales Value wotax', 'Sales Value wtax', 'Currency Code', 'Distributor', 'Country', 
        'Inventory Unit']

    df_entrepidus = df_entrepidus.reindex(columns=entrepidus_columns)
    #df_entrepidus = df_entrepidus.sort_values(by='Date', ascending=False)

    return df_entrepidus

def verifying_values_with_without_tax(df_entrepidus):

    df_entrepidus['Sales Value wtax'] = pd.to_numeric(df_entrepidus['Sales Value wtax'], errors='coerce').fillna(0)
    df_entrepidus['Sales Value wotax'] = pd.to_numeric(df_entrepidus['Sales Value wotax'], errors='coerce').fillna(0)

    sum_value_with_tax = df_entrepidus['Sales Value wtax'].sum()
    sum_value_without_tax = df_entrepidus['Sales Value wotax'].sum()

    if ( sum_value_without_tax > sum_value_with_tax ):

        df_entrepidus.rename(columns={ 'Sales Value wtax':'Sales Value wotax', 'Sales Value wotax':'Sales Value wtax' }, inplace=True)

    return df_entrepidus

def loading_stock_file(entrepidus_stock_file_path):

    found_entrepidus_stock = True

    try:
        df_entrepidus_stock = pd.read_csv( entrepidus_stock_file_path, encoding='mbcs', index_col=False, sep=';', low_memory=False,
            dtype=str ).fillna('')
    except:
        logger.logger.info('No stock file found on {}'.format(entrepidus_stock_file_path))
        print('Entrepidus_stock not found for this distributor!')
        found_entrepidus_stock = False

    if (found_entrepidus_stock == True):
        return [found_entrepidus_stock, df_entrepidus_stock]
    else:
        return [ found_entrepidus_stock ]


def formatting_stock_file(df_entrepidus_stock):

    df_entrepidus_stock = df_entrepidus_stock.assign(Diageo_dist_auxiliar_column = '-')

    df_entrepidus_stock.columns = [column.encode('mbcs').decode('mbcs', 'ignore') for column in df_entrepidus_stock.columns]
    
    try:
        df_entrepidus_stock['Inventory Unit'] = pd.to_numeric(df_entrepidus_stock['Inventory Unit']).fillna(0)
    except:
        print('Not possible changing to Numeric column Inventory Unit of df_entrepidus_stock')
        logger.logger.error('Not possible changing to Numeric column Inventory Unit of df_entrepidus_stock')

    entrepidus_stock_columns = ['Diageo_dist_auxiliar_column', 'Date', 'Store Number', 'Store Name', 'Chain', 'Supervisor', 'Region',
        'Commune', 'Merchandiser', 'Chain SKU Code', 'Diageo SKU Code',	'Desc Producto & Cód.',
        'Category', 'Sub Category', 'Brand', 'Brand Variant', 'Unit Size', 'Unit Sold', 
        'Sales Value wotax', 'Sales Value wtax', 'Currency Code', 'Distributor', 'Country', 
        'Inventory Unit']

    df_entrepidus_stock = df_entrepidus_stock.reindex(columns=entrepidus_stock_columns)

    return df_entrepidus_stock


def appending_entrepidus_stock_to_entrepidus_sales(df_entrepidus_stock, df_entrepidus):

    try:
        df_entrepidus = df_entrepidus.append(df_entrepidus_stock, ignore_index=True)
    except:
        logger.logger.error('Not posible appending Stock to Entrepidus')
    
    return df_entrepidus

# Creating Excel flie -------
def creating_csv_files(df_entrepidus, df_new_stores, root_path):

    today_date = datetime.today()
    today_date = today_date.strftime("%Y%m%d")    
    csv_entrepidus_file_path = root_path + '/EntrepidusDistributors_' + today_date + '_automated.csv'
    csv_customer_file_path = root_path + '/Customers Catalogue_automated.csv'

    try:
        df_entrepidus[df_entrepidus.columns].to_csv(csv_entrepidus_file_path, encoding='mbcs', sep=';',
        columns=df_entrepidus.columns, index=False)
    except:
        print('Not possible creating EntrepidusDistributors CSV File')
        logger.logger.error('Not possible creating EntrepidusDistributors CSV File')
    
    try:
        df_new_stores.to_csv(csv_customer_file_path, sep=';', encoding='mbcs', index=False)
    except:
        print('Not possible creating Customer_catalogue CSV File')
        logger.logger.error('Not possible creating Customer_catalogue CSV File')

def main():

    try:
        user_inputs = getting_user_input()
        root_path = user_inputs[0]
        country = user_inputs[1]
        STR_indicator = user_inputs[2]
    except:
        print('Not possible getting user input')
        os.system('pause')
        sys.exit()

    try:
        system_paths_dataframes_and_root_path = getting_system_paths(root_path, country, STR_indicator)
        system_paths = system_paths_dataframes_and_root_path[:5]
        root_path = system_paths_dataframes_and_root_path[5]
        entrepidus_stock_file_path = system_paths_dataframes_and_root_path[6]
        store_txt_file_path = system_paths_dataframes_and_root_path[7]
        customer_filling_reference_file_path = system_paths_dataframes_and_root_path[8]
    except:
        logger.logger.error('Not possible  getting_system_paths')
        print('Not possible getting_system_paths')
        os.system('pause')
        sys.exit()

    try:
        print('Loading data frames...')
        dataframes = loading_dataframes(system_paths)
        df_sales = dataframes[0]
        df_pebac_product_reference = dataframes[1]
        df_product_master = dataframes[2]
        df_customer_catalog = dataframes[3]
        df_dist_names = dataframes[4]
    except:
        logger.logger.error('Not possible loading DataFrames')
        print('Not possible loading DataFrames')
        os.system('pause')
        sys.exit()

    try:
        print('Cleaning sales.txt file...')
        df_sales = sanitizing_sales_file(df_sales)
    except:
        logger.logger.error('Not possible sanitizing_sales_file')
        print('Not able to execute - sanitizing_sales_file function')
        os.system('pause')
        sys.exit()

    try:
        print('Cleaning df_pebac_product_reference...')
        df_pebac_product_reference = sanitizing_df_pebac_product_reference(df_pebac_product_reference)
    except:
        logger.logger.error('Not possible sanitizing_df_pebac_product_reference function')
        print('Not possible execute sanitizing_df_pebac_product_reference function')
        os.system('pause')
        sys.exit()

    try:
        print('Setting Entrepidus...')
        df_entrepidus = declaring_entrepidus_df()
    except:
        logger.logger.error('Not possible creating Entrepidus')
        print('Not possible creating Entrepidus')
        os.system('pause')
        sys.exit()

    try:
        print('Assigning sales to entrepidus...')
        dataframes_entrepidus_and_sales = setting_df_entrepidus_and_sales(df_entrepidus, df_sales)
        df_entrepidus = dataframes_entrepidus_and_sales[0]
        df_sales = dataframes_entrepidus_and_sales[1]
    except:
        logger.logger.error('Not possible executing function setting_df_entrepidus_and_sales')
        print('Not possible executing function setting_df_entrepidus_and_sales')
        os.system('pause')
        sys.exit()

    try:
        print('assigning_dist_names_and_country_to_entrepidus')
        df_entrepidus = assigning_dist_names_and_country_to_entrepidus(df_entrepidus, df_dist_names)
    except:
        logger.logger.error('Not possible executing function setting_df_entrepidus_and_sales')
        print('Not possible assigning dist_names_and_country to entrepidus')

    try:
        print('Filtering current and previous month...')
        previous_and_current_month_period = get_previous_and_current_month_period()
    except: 
        logger.logger.error('Not possible executing function setting_df_entrepidus_and_sales')
        print('Not possible executing function setting_df_entrepidus_and_sales')

    try:
        print('Searching Diageo Skus...')
        df_entrepidus = searching_diageo_sku(df_sales, df_pebac_product_reference, df_entrepidus)
    except:
        logger.logger.error('Not possible executing function searching_diageo_sku')
        print('Not possible executing function searching_diageo_sku')
        os.system('pause')
        sys.exit()

    try:
        print('Filling product details...')
        df_entrepidus = filling_product_details(df_entrepidus, df_product_master)
    except:
        logger.logger.error('Not possible executing function filling_product_details')
        print('Not possible filling_product_details')
        os.system('pause')
        sys.exit()

    try:
        print('Calculating quantity...')
        df_entrepidus = calculating_quantity(df_entrepidus, df_pebac_product_reference)
    except:
        logger.logger.error('Not possible executing function calculating_quantity')
        print('Not possible calculating products quantities using pebac_product_reference file')
        os.system('pause')
        sys.exit()

    try:
        print('Getting store names...')
        mapping_stores = getting_store_name(df_entrepidus, df_customer_catalog)
        df_entrepidus = mapping_stores[0]
        new_stores = mapping_stores[1]
    except:
        logger.logger.error('Not possible executing function getting_store_name')
        print('Not possible getting store names')
        os.system('pause')
        sys.exit()
    
    try:
        print('Creating new stores dataframe...')
        df_new_stores = creating_new_stores_dataframe()
    except:
        logger.logger.error('Not possible executing function creating_new_stores_dataframe')
        print('Not possible creating_new_stores_dataframe')

    try:
        print('Registering new stores...')
        df_new_stores = registering_new_stores(new_stores, df_new_stores)
    except:
        logger.logger.error('Not possible executing function registering_new_stores')
        print('Not possible executing function registering_new_stores')
    

    result_loading_store_txt_file_and_customer_filling_reference = True
    if (len(new_stores) > 0):
        try:
            print('loading_store_txt_file and customer_filling_reference')
            store_txt_and_customer_filling_reference = loading_store_txt_file_and_customer_filling_reference(
                store_txt_file_path, 
                STR_indicator,
                customer_filling_reference_file_path)
        except:
            result_loading_store_txt_file_and_customer_filling_reference = False
            logger.logger.error('Not possible loading_store_txt_file_and_customer_filling_reference')
            print('Not possible loading_store_txt_file_and_customer_filling_reference')
            print('** Please verify if the store.txt file is encoded as UTF-8 **')
        finally:
            if result_loading_store_txt_file_and_customer_filling_reference:
                df_store_txt_flat_file = store_txt_and_customer_filling_reference[0]
                df_z_customer_standard_filling_reference = store_txt_and_customer_filling_reference[1]
    else:
        result_loading_store_txt_file_and_customer_filling_reference = False


    if result_loading_store_txt_file_and_customer_filling_reference:
        try:
            print('sanitizing df_store_txt_flat_file')
            df_store_txt_flat_file = sanitizing_df_store_txt_flat_file(df_store_txt_flat_file)
        except:
            logger.logger.error('Not possible sanitizing_df_store_txt_flat_file')
            print('sanitizing_df_store_txt_flat_file')


    declaring_dictionaries_result = True
    try:
        dictionaries = declaring_dictionaries()
    except:
        declaring_dictionaries_result = False
        logger.logger.error('Not possible declaring_dictionaries')
        print('Not possible declaring_dictionaries')
    finally:
        if declaring_dictionaries_result:
            try:
                dict_store_vs_customer_catalog_A = dictionaries[0]
                dict_store_vs_customer_catalog_B = dictionaries[1]
            except:
                print('Not possible assigning dictionaries')


    if result_loading_store_txt_file_and_customer_filling_reference:   
        try:
            print('filling new stores details')
            df_new_stores = filling_new_stores_details(df_new_stores,
                df_store_txt_flat_file,
                df_z_customer_standard_filling_reference,
                dict_store_vs_customer_catalog_A,
                dict_store_vs_customer_catalog_B)       
        except:
            logger.logger.error('Not possible filling_new_stores_details')
            print('Error filling_new_stores_details')


    try:
        print('Checking tax values columns...')
        df_entrepidus = verifying_values_with_without_tax(df_entrepidus)
    except:
        logger.logger.error('Not possible verifying_values_with_without_tax(df_entrepidus)')
        print('Not possible verifying_values_with_without_tax(df_entrepidus')
    
    try:
        print('Formatting Entrepidus...')
        df_entrepidus = entrepidus_formatting(df_entrepidus)
    except:
        logger.logger.error('Not possible executing function entrepidus_formatting')
        print('Not possible formatting Entrepidus')
        os.system('pause')
        sys.exit()
    
    try:
        print('Searching stock file...')
        result_finding_stock_file = loading_stock_file(entrepidus_stock_file_path)
    except:
        logger.logger.info('Not possible executing loading_stock_file')
    finally:
        found_stock_file = result_finding_stock_file[0]
        if ( found_stock_file == True ):
            try:
                df_entrepidus_stock = result_finding_stock_file[1]
            except:
                logger.logger.info('Not possible creating DataFrame df_entrepidus_stock')
                print('Not possible creating DataFrame df_entrepidus_stock')
    
    #Just getting into that function if df_stock is not false
    if found_stock_file:
        try:
            print('Checking stock file...')
            df_entrepidus_stock = formatting_stock_file(df_entrepidus_stock)
        except:
            logger.logger.info('Not possible executing formatting_stock_file')
    
    #Just getting into that function if df_stock is not false
    if found_stock_file:
        try:
            print('Appending stock file into entrepidus sales...')
            df_entrepidus = appending_entrepidus_stock_to_entrepidus_sales(df_entrepidus_stock, df_entrepidus)
        except:
            logger.logger.info('Not possible executing formatting_stock_file')
            print('Not posible appending Stock to Entrepidus')

    try:  
        print('Creating CSV files...')
        creating_csv_files(df_entrepidus, df_new_stores, root_path)
    except:
        logger.logger.error('Not possible executing function creating_excel_file')
        print('Not possible executing function creating_excel_file')
        print('** Please make sure that a previous generated Entrepidus in the same folder is not open **\n')
        os.system('pause')
        sys.exit()


    print('Successfully executed')
    os.system('pause')


if __name__ == '__main__':
  main()
