import unittest
import pandas as pd 
import numpy as np 
import Entrepidus_generator

class TestEntrepidus(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        
        cls.df_sales_columns = ['Country', 'Diageo Customer ID', 'Diageo Customer Name', 
            'Invoice number', 'Type of Invoice', 'Invoice Date', 'Store code', 'Product Code', 
            'Quantity', 'Unit of measure', 'Total Amount WITHOUT TAX', 'Total Amount WITH TAX', 
            'Currency Code', 'Sales Representative Code']

        cls.df_pebac_product_reference_columns = ['Dist_Code', 'Distributor', 'Product_store_id', 'Country', 'Diageo_Sku',
            'Relevant', 'Scale']

        cls.df_entrepidus_columns = ['Date', 'Store Number', 'Store Name', 'Chain', 'Supervisor', 'Region',
        'Commune', 'Merchandiser', 'Chain SKU Code', 'Diageo SKU Code',	'Desc Producto & Cód.',
        'Category', 'Sub Category', 'Brand', 'Brand Variant', 'Unit Size', 'Unit Sold', 
        'Sales Value wotax', 'Sales Value wtax', 'Currency Code', 'Distributor', 'Country', 
        'Inventory Unit', 'Diageo_dist_auxiliar_column', 'Aux_product_relevance']

        cls.df_dist_names_columns = ['Distributor_country', 'Distributor_id', 'Distributor_name']


    def test_getting_user_input(self):

        root_path = 'C:/Users/BACELKEN/Documents/Automation'
        catalogs_root_path = '../../../Catalogs/Traditional_STR/'
        product_by_distributor_file_name = 'pebac_ref_prod.xlsx'
        STR_indicator = False
        country = 'Argentina'

        sales_file_path = root_path + '/sales.txt'
        store_txt_file_path = root_path + '/store.txt'
        pebac_master_data_product_file_path = catalogs_root_path + 'Product_catalog/' + product_by_distributor_file_name
        product_master_path = catalogs_root_path + 'Product_catalog/product_master.xlsx'
        customer_catalog_file_path = catalogs_root_path + 'Customer_catalog/' + country + '_customer_catalog.xlsx'
        dist_names_file_path = catalogs_root_path + 'dist_names.xlsx'
        customer_filling_reference_file_path = catalogs_root_path + 'Customer_catalog/z_customer_reference.xlsx'

        entrepidus_stock_directory_path = '/'.join(root_path.split('/')[:-1])
        entrepidus_stock_file_path = entrepidus_stock_directory_path + '/Entrepidus_STOCK.csv'

        expected_paths = [sales_file_path, pebac_master_data_product_file_path, 
            product_master_path, customer_catalog_file_path, dist_names_file_path, root_path,
            entrepidus_stock_file_path, store_txt_file_path, customer_filling_reference_file_path]

        
        self.assertEqual(Entrepidus_generator.getting_system_paths(root_path, 
            country, STR_indicator), expected_paths)    


    def test_sanitizing_sales_file(self):

        df_sales = pd.DataFrame(columns=self.df_sales_columns)
        
        df_sales['Quantity'] = ['55-', 37]
        df_sales['Total Amount WITH TAX'] = ['88-','']
        df_sales['Total Amount WITHOUT TAX'] = [77, '56.7-']
        df_sales['Product Code'] = ['0034', '877']
        df_sales['Store code'] = ['001111111111159', '123']
        df_sales.fillna('', inplace=True)

        df_expected = pd.DataFrame(columns=self.df_sales_columns)

        df_expected['Quantity'] = [-55, 37]
        df_expected['Total Amount WITH TAX'] = [-88.0, 0]
        df_expected['Total Amount WITHOUT TAX'] = [77, -56.7]
        df_expected['Product Code'] = ['34', '877']
        df_expected['Store code'] = ['111111111115', '123']
        df_expected.fillna('', inplace=True)

        pd.testing.assert_frame_equal(Entrepidus_generator.sanitizing_sales_file(df_sales), df_expected)


    def test_sanitizing_df_pebac_product_reference(self):

        df_pebac_product_reference = pd.DataFrame(columns=self.df_pebac_product_reference_columns)
        
        #Creating expected DataFrame
        df_expected = pd.DataFrame(columns=self.df_pebac_product_reference_columns)
        df_expected['Scale'] = [6.0, 1.0]

        #Setting values in Scale column - to be tested
        df_pebac_product_reference['Scale'] = [6, np.nan]
        #Simulating a special character in the DataFrame header
        df_pebac_product_reference.rename(columns={ 'Country':'�Country'}, inplace=True)

        pd.testing.assert_frame_equal(Entrepidus_generator.sanitizing_df_pebac_product_reference(df_pebac_product_reference), df_expected)

    
    def test_setting_df_entrepidus_and_sales(self):
        
        df_sales = pd.DataFrame(columns=self.df_sales_columns)
        df_entrepidus = pd.DataFrame(columns=self.df_entrepidus_columns)
        df_expected = pd.DataFrame(columns=self.df_entrepidus_columns)

        df_sales_update = {
            'Country': 'Argentina',
            'Total Amount WITHOUT TAX': -56.8,
            'Total Amount WITH TAX': -17.0,
            'Currency Code': 'ARS',
            'Store code': '123',
            'Invoice Date': '20200813',
            'Product Code': '555444',
            'Diageo Customer Name': 'Peñaflor',
            'Quantity': 55.0,
            'Inventory Unit': 0,
            'Diageo Customer ID': '123456',
            'Unit of measure': 'BTL'
        }
        df_sales = df_sales.append(df_sales_update, ignore_index=True)

        df_expected_to_be_updated = {
            'Country': 'Argentina',
            'Sales Value wotax': -56.8,
            'Sales Value wtax': -17.0,
            'Currency Code': 'ARS',
            'Store Number': '123',
            'Date': '20200813',
            'Chain SKU Code': '555444',
            'Distributor': 'Peñaflor',
            'Unit Sold': 55.0,
            'Inventory Unit': 0,
            'Diageo_dist_auxiliar_column': '123456',
            'Aux_unit_of_measure': 'btl'
        }
        df_expected = df_expected.append(df_expected_to_be_updated, ignore_index=True)
        
        pd.testing.assert_frame_equal(Entrepidus_generator.setting_df_entrepidus_and_sales(df_entrepidus, df_sales), df_expected, check_dtype=False)


    def test_assigning_dist_names_and_country_to_entrepidus(self):

        df_dist_names = pd.DataFrame(columns=self.df_dist_names_columns)
        df_entrepidus = pd.DataFrame(columns=self.df_entrepidus_columns)
        df_expected = pd.DataFrame(columns=self.df_entrepidus_columns)

        df_dist_names['Distributor_country'] = ['Peru']
        df_dist_names['Distributor_id'] = ['288039']
        df_dist_names['Distributor_name'] = ['Jandy']

        df_entrepidus['Diageo_dist_auxiliar_column'] = ['288039']
        df_entrepidus['Distributor'] = ['Jandiii']
        df_entrepidus['Country'] = ['PERIVIS']

        df_expected['Diageo_dist_auxiliar_column'] = ['288039']
        df_expected['Distributor'] = ['Jandy']
        df_expected['Country'] = ['Peru']

        returned_df_entrepidus = Entrepidus_generator.assigning_dist_names_and_country_to_entrepidus(df_entrepidus, df_dist_names)

        df_expected = df_expected.sort_index(axis=1, ascending=True)
        returned_df_entrepidus = returned_df_entrepidus.sort_index(axis=1, ascending=True)

        pd.testing.assert_frame_equal(returned_df_entrepidus, df_expected)


if __name__ == '__main__':
    unittest.main()