import pandas as pd 
import os
import sys
from datetime import datetime, date
import warnings
warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)

def getting_user_input():
    print('*** Stock generator ***')

    root_path = input('Please inform the root path: \n')

    return (True, [root_path])


def defining_file_paths(root_path):

    catalogs_root_path = '../../../Catalogs/Traditional_STR/'

    prod_ref_file_path = catalogs_root_path + 'Product_catalog/pebac_ref_prod.xlsx'
    dist_names_file_path = catalogs_root_path + 'dist_names.xlsx'
    product_master_path = catalogs_root_path + 'Product_catalog/product_master.xlsx'
    stock_file_path = root_path + '/stock.txt'

    return (True, [stock_file_path, prod_ref_file_path, product_master_path, dist_names_file_path])


def loading_stock_and_prod_ref_files(stock_file_path, prod_ref_file_path, product_master_path, dist_names_file_path):

    #Defining stock columns and loading stock.txt file
    stock_file_columns = [
        'Country', 'Product Code', 'Diageo Customer ID', 'Diageo Customer Name',	
        'Invoice Date', 'Quantity',	'Unit of measure', 'Stock Status', 'Warehouse'
        ]
    try:
        df_stock = pd.read_csv(stock_file_path, index_col=False, names=stock_file_columns,sep=';', low_memory=False,
            dtype={ 'Quantity':str, 'Product Code':str, 
            'Invoice Date':str }, header=None).fillna('')
    except Exception as error:
        print(error)
        raise Exception(error)

    #Defining prob_ref columns and loading prod_ref file
    try:
        df_pebac_product_reference = pd.read_excel(prod_ref_file_path, 
            converters = { 'Dist_Code': str, 'Product_store_id': str} ).fillna('')
    except Exception as error:
        print(error)
        raise Exception(error)
    
    #Loading dist names file
    try:
        df_dist_names = pd.read_excel(dist_names_file_path, dtype=str ).fillna('')
    except Exception as error:
        print(error)
        raise Exception(error)

    #Loading Data Frame of Product Master Data
    try:
        df_product_master = pd.read_excel(product_master_path, dtype={ 'Material': str }).fillna('')      
    except:
        print('Not possible opening the file {}'.format(product_master_path))



    return (True, [df_stock, df_pebac_product_reference, df_product_master, df_dist_names])


def creating_stock_entrepidus():

    stock_entrepidus_columns = ['Date', 'Store Number', 'Store Name', 'Chain', 'Supervisor', 'Region',
        'Commune', 'Merchandiser', 'Chain SKU Code', 'Diageo SKU Code',	'Desc Producto & Cód.',
        'Category', 'Sub Category', 'Brand', 'Brand Variant', 'Unit Size', 'Unit Sold', 
        'Sales Value wotax', 'Sales Value wtax', 'Currency Code', 'Distributor', 'Country', 
        'Inventory Unit', 'Diageo_dist_auxiliar_column', 'Aux_product_relevance']
    
    try:
        df_entrepidus_stock = pd.DataFrame(columns=stock_entrepidus_columns).fillna('')
    except Exception as error:
        print(error)
        raise Exception(error)
    
    return (True, [df_entrepidus_stock])


def assigning_stock_to_entrepidus(df_stock, df_entrepidus_stock):

    try:
        df_entrepidus_stock['Diageo_dist_auxiliar_column'] = df_stock['Diageo Customer ID']
        df_entrepidus_stock['Date'] = df_stock['Invoice Date']
        df_entrepidus_stock['Store Number'] = '1stock'
        df_entrepidus_stock['Store Name'] = df_stock['Diageo Customer Name']
        df_entrepidus_stock['Chain SKU Code'] = df_stock['Product Code']
        df_entrepidus_stock['Unit Sold'] = 0
        df_entrepidus_stock['Sales Value wotax'] = 0
        df_entrepidus_stock['Sales Value wtax'] = 0
        df_entrepidus_stock['Distributor'] = df_stock['Diageo Customer Name']
        df_entrepidus_stock['Country'] = df_stock['Country']
        df_entrepidus_stock['Inventory Unit'] = df_stock['Quantity']
    except Exception as error:
        print(error)
        raise Exception(error)

    return (True, [df_entrepidus_stock])


def sanitizing_entrepidus_stock(df_entrepidus_stock):

    df_entrepidus_stock['Chain SKU Code'] = df_entrepidus_stock['Chain SKU Code'].str.lstrip('0')
    df_entrepidus_stock['Chain SKU Code'] = df_entrepidus_stock['Chain SKU Code'].str.strip()

    return (True, [df_entrepidus_stock])


def assigning_dist_names_information(df_entrepidus_stock, df_dist_names):

    df_entrepidus_stock.set_index(['Diageo_dist_auxiliar_column'], inplace=True)
    df_entrepidus_stock.index = df_entrepidus_stock.index.map(str)

    df_dist_names.set_index(['Distributor_id'], inplace=True)
    df_dist_names.index = df_dist_names.index.map(str)
    df_dist_names = df_dist_names[~df_dist_names.index.duplicated(keep='first')]

    for single_distributor in df_entrepidus_stock.index.unique():

        try:
            distributor_correct_name = df_dist_names.loc[single_distributor, 'Distributor_name']
            distributor_correct_country = df_dist_names.loc[single_distributor, 'Distributor_country']
        except Exception as error:
            print(error)

        try:
            df_entrepidus_stock.loc[single_distributor, 'Distributor'] = distributor_correct_name
        except Exception as error:
            print(error)
        
        try:
            df_entrepidus_stock.loc[single_distributor, 'Country'] = distributor_correct_country
        except Exception as error:
            print(error)

    df_entrepidus_stock.reset_index(inplace=True)
    df_dist_names.reset_index(inplace=True)
    return (True, [df_entrepidus_stock])


def searching_diageo_sku(df_entrepidus_stock, df_pebac_product_reference):

    df_entrepidus_stock = df_entrepidus_stock.set_index(['Diageo_dist_auxiliar_column', 'Chain SKU Code'])
    df_entrepidus_stock.index = df_entrepidus_stock.index.set_levels(df_entrepidus_stock.index.levels[0].astype(str), level=0)
    df_entrepidus_stock.index = df_entrepidus_stock.index.set_levels(df_entrepidus_stock.index.levels[1].astype(str), level=1)

    df_pebac_product_reference.set_index(['Dist_Code', 'Product_store_id'], inplace=True)
    df_pebac_product_reference = df_pebac_product_reference[~df_pebac_product_reference.index.duplicated(keep='last')]   
    
    for single_distributor, single_product_by_distributor in df_entrepidus_stock.index.unique():

        try:
            diageo_sku = df_pebac_product_reference.loc[(single_distributor, single_product_by_distributor), 'Diageo_Sku']
            df_entrepidus_stock.loc[(single_distributor, single_product_by_distributor), 'Diageo SKU Code'] = diageo_sku
        except Exception as error:
            df_entrepidus_stock.loc[(single_distributor, single_product_by_distributor), 'Diageo SKU Code'] = '0000 - NOT FOUND'
            print(error)

        try:
            product_relevance = df_pebac_product_reference.loc[(single_distributor, single_product_by_distributor), 'Relevant']
            df_entrepidus_stock.loc[( single_distributor, single_product_by_distributor ), 'Aux_product_relevance'] = product_relevance
        except Exception as error:
            print(error)

    df_entrepidus_stock.reset_index(inplace = True)
    
    return (True, [df_entrepidus_stock])


#Filling Entrepidus with the product details
def filling_product_details(df_entrepidus_stock, df_product_master):

    df_product_master.set_index(['Material'], inplace=True)
    df_product_master.index = df_product_master.index.map(str) #Changing indexes into string
    df_product_master = df_product_master[~df_product_master.index.duplicated(keep='last')]

    df_entrepidus_stock.set_index(['Diageo SKU Code'], inplace=True)
    df_entrepidus_stock.index = df_entrepidus_stock.index.map(str) #Changing indexes into string

    for specific_diageo_sku in df_entrepidus_stock.index.unique():
        
        try:
            df_entrepidus_stock['Desc Producto & Cód.'].loc[specific_diageo_sku] = df_product_master['Description'].loc[specific_diageo_sku]
            df_entrepidus_stock['Category'].loc[specific_diageo_sku] = df_product_master['Main Group'].loc[specific_diageo_sku]
            df_entrepidus_stock['Sub Category'].loc[specific_diageo_sku] = df_product_master['Subcategory'].loc[specific_diageo_sku]
            df_entrepidus_stock['Brand'].loc[specific_diageo_sku] = df_product_master['Brand'].loc[specific_diageo_sku]
            df_entrepidus_stock['Brand Variant'].loc[specific_diageo_sku] = df_product_master['Brand Variant'].loc[specific_diageo_sku]
            df_entrepidus_stock['Unit Size'].loc[specific_diageo_sku] = df_product_master['Unit Size'].loc[specific_diageo_sku]
        except Exception as error:
            print('{}\n {} - Not possible filling this product details'.format(error, specific_diageo_sku))
        
    df_entrepidus_stock.reset_index(inplace=True)

    return (True, [df_entrepidus_stock])


#Filling Entrepidus with quantities (Unit sold - after multiplying for the product tx)
def calculating_quantity(df_entrepidus_stock, df_pebac_product_reference):

    df_pebac_product_reference = df_pebac_product_reference[~df_pebac_product_reference.index.duplicated(keep='last')]

    df_entrepidus_stock.set_index(['Diageo_dist_auxiliar_column', 'Chain SKU Code'], inplace=True)
    df_entrepidus_stock.index = df_entrepidus_stock.index.set_levels(df_entrepidus_stock.index.levels[0].astype(str), level=0)
    df_entrepidus_stock.index = df_entrepidus_stock.index.set_levels(df_entrepidus_stock.index.levels[1].astype(str), level=1)

    for single_distributor, single_product in df_entrepidus_stock.index.unique():

        try:
            multiplicative_factor = df_pebac_product_reference.loc[( single_distributor , single_product ), 'Scale']
        except Exception as error:
            print(error)
            multiplicative_factor = 1

        try:
            df_entrepidus_stock.loc[( single_distributor , single_product ), 'Inventory Unit'] = df_entrepidus_stock.loc[( single_distributor , single_product ), 'Inventory Unit']*(multiplicative_factor)
        except Exception as error:
            print(error)
            
    try:
        df_entrepidus_stock['Inventory Unit'] = df_entrepidus_stock['Inventory Unit'].round(0).astype(int)
    except Exception as error:
        print(error)

    df_entrepidus_stock.reset_index(inplace=True)

    return (True, [df_entrepidus_stock])


def formatting_stock_file(df_entrepidus_stock):

    try:
        df_entrepidus_stock['Inventory Unit'] = pd.to_numeric(df_entrepidus_stock['Inventory Unit'], errors='coerce').fillna(0)
    except Exception as error:
        print(error)

    entrepidus_stock_columns = ['Diageo_dist_auxiliar_column', 'Date', 'Store Number', 'Store Name', 'Chain', 'Supervisor', 'Region',
        'Commune', 'Merchandiser', 'Chain SKU Code', 'Diageo SKU Code',	'Desc Producto & Cód.',
        'Category', 'Sub Category', 'Brand', 'Brand Variant', 'Unit Size', 'Unit Sold', 
        'Sales Value wotax', 'Sales Value wtax', 'Currency Code', 'Distributor', 'Country', 
        'Inventory Unit']

    df_entrepidus_stock = df_entrepidus_stock.reindex(columns=entrepidus_stock_columns)

    return (True, [df_entrepidus_stock])


def discarding_non_relevant_products(df_entrepidus_stock):

    df_entrepidus_stock = df_entrepidus_stock.drop(df_entrepidus_stock[df_entrepidus_stock['Aux_product_relevance'] == 'N'].index)
    return (True, [df_entrepidus_stock])


def creating_csv_files(df_entrepidus_stock, root_path):

    today_date = datetime.today()
    today_date = today_date.strftime("%Y%m%d")    
    csv_entrepidus_stock_file_path = root_path + '/stock_EntrepidusDistributors_' + today_date + '_automated.csv'

    try:
        df_entrepidus_stock[df_entrepidus_stock.columns].to_csv(csv_entrepidus_stock_file_path, encoding='mbcs', sep=';',
        columns=df_entrepidus_stock.columns, index=False)
    except Exception as error:
        print(error)
        return (False, [])
    
    return (True, [])

def main():

    try:
        print('getting_user_input...')
        success_getting_user_input, content_getting_user_input = getting_user_input()
    except Exception as error:
        print(error)
        sys.exit()
    finally:
        if success_getting_user_input:
            root_path = content_getting_user_input[0]
    
    try:
        print('defining_file_paths...')
        success_defining_file_paths, content_defining_file_paths = defining_file_paths(root_path)
    except Exception as error:
        print(error)
        sys.exit()
    finally:
        if success_defining_file_paths:
            stock_file_path = content_defining_file_paths[0]
            prod_ref_file_path = content_defining_file_paths[1]
            product_master_path = content_defining_file_paths[2]
            dist_names_file_path = content_defining_file_paths[3]
    
    try:
        print('loading_stock_and_prod_ref_files...')
        success_loading_stock_and_prod_ref_files, content_loading_stock_and_prod_ref_files = loading_stock_and_prod_ref_files(stock_file_path,
            prod_ref_file_path, product_master_path, dist_names_file_path)
    except Exception as error:
        print(error)
        sys.exit()
    finally:
        if success_loading_stock_and_prod_ref_files:
            df_stock = content_loading_stock_and_prod_ref_files[0]
            df_pebac_product_reference = content_loading_stock_and_prod_ref_files[1]
            df_product_master = content_loading_stock_and_prod_ref_files[2]
            df_dist_names = content_loading_stock_and_prod_ref_files[3]
    
    try:
        print('creating_stock_entrepidus...')
        success_creating_stock_entrepidus, content_creating_stock_entrepidus = creating_stock_entrepidus()
    except Exception as error:
        print(error)
        sys.exit()
    finally:
        if success_creating_stock_entrepidus:
            df_entrepidus_stock = content_creating_stock_entrepidus[0]
    
    try:
        print('assigning_stock_to_entrepidus...')
        success_assigning_stock_to_entrepidus, content_assigning_stock_to_entrepidus = assigning_stock_to_entrepidus(df_stock, df_entrepidus_stock)
    except Exception as error:
        print(error)
    finally:
        if success_assigning_stock_to_entrepidus:
            df_entrepidus_stock = content_assigning_stock_to_entrepidus[0]
    
    try:
        print('Sanitizing Stock_Entrepidus')
        success_sanitizing_entrepidus_stock, content_sanitizing_entrepidus_stock = sanitizing_entrepidus_stock(df_entrepidus_stock)
    except Exception as error:
        print('{} - Not possible sanitizing stock entrepidus'.format(error))
        sys.exit()
    finally:
        if success_sanitizing_entrepidus_stock:
            df_entrepidus_stock = content_sanitizing_entrepidus_stock[0]

    try:
        print('assigning dist_names_information')
        success_assigning_dist_names_information, content_assigning_dist_names_information = assigning_dist_names_information(df_entrepidus_stock, df_dist_names)
    except Exception as error:
        print(error)
    finally:
        if success_assigning_dist_names_information:
            df_entrepidus_stock = content_assigning_dist_names_information[0]
    
    try:
        print('searching_diageo_sku...')
        success_searching_diageo_sku, content_searching_diageo_sku = searching_diageo_sku(df_entrepidus_stock, df_pebac_product_reference)
    except Exception as error:
        print('{}\n error searching_diageo_sku...'.format(error))
        sys.exit()
    finally:
        if success_searching_diageo_sku:
            df_entrepidus_stock = content_searching_diageo_sku[0]
    
    try:
        print('discarding_non_relevant_products')
        sucess_discarding_non_relevant_products, content_discarding_non_relevant_products = discarding_non_relevant_products(df_entrepidus_stock)
    except Exception as error:
        print(error)
    finally:
        if sucess_discarding_non_relevant_products:
            df_entrepidus_stock = content_discarding_non_relevant_products[0]
    
    try:
        print('filling_product_details...')
        success_filling_product_details, content_filling_product_details = filling_product_details(df_entrepidus_stock, df_product_master)
    except Exception as error:
        print('{} - Not possible filling_product_details!'.format(error))
        sys.exit()
    finally:
        if success_filling_product_details:
            df_entrepidus_stock = content_filling_product_details[0]

    try:    
        print('calculating quantity...')
        success_calculating_quantity, content_calculating_quantity = calculating_quantity(df_entrepidus_stock, df_pebac_product_reference)
    except Exception as error: 
        print('error quantity')
        print(error)
    finally:
        if success_calculating_quantity:
            df_entrepidus_stock = content_calculating_quantity[0]
    
    try:
        print('formatting_stock_file(df_entrepidus_stock)')
        success_formatting_stock_file, content_formatting_stock_file = formatting_stock_file(df_entrepidus_stock)
    except Exception as error:
        print(error)
    finally:
        if success_formatting_stock_file:
            df_entrepidus_stock = content_formatting_stock_file[0]

    try:
        print('creating_csv_files')
        success_creating_csv_files, content_creating_csv_files = creating_csv_files(df_entrepidus_stock, root_path)
    except Exception as error:
        print(error)
        sys.exit()
    finally:
        if success_creating_csv_files:
            print('Successfully finished!')


if __name__ == '__main__':
  main()
