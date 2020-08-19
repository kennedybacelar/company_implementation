import unittest
import pandas as pd 
import numpy as np 
import Entrepidus_generator
import stock_generator

class TestStockGenerator(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.stock_file_columns = [
        'Country', 'Product Code', 'Diageo Customer ID', 'Diageo Customer Name',	
        'Invoice Date', 'Quantity',	'Unit of measure', 'Stock Status', 'Warehouse'
        ]

        cls.stock_entrepidus_columns = ['Date', 'Store Number', 'Store Name', 'Chain', 
        'Supervisor', 'Region','Commune', 'Merchandiser', 'Chain SKU Code', 'Diageo SKU Code',	
        'Desc Producto & Cód.','Category', 'Sub Category', 'Brand', 'Brand Variant', 
        'Unit Size', 'Unit Sold', 'Sales Value wotax', 'Sales Value wtax', 
        'Currency Code', 'Distributor', 'Country', 'Inventory Unit', 
        'Diageo_dist_auxiliar_column', 'Aux_product_relevance'
        ]

        cls.dist_names_columns = ['Distributor_country', 'Distributor_id', 'Distributor_name']

        cls.df_pebac_product_reference_columns = ['Dist_Code', 'Product_store_id', 'Diageo_Sku',
            'Relevant', 'Scale']

        cls.df_product_master_columns = ['Material', 'Description', 
            'Main Group', 'Subcategory', 'Brand', 'Brand Variant', 'Unit Size']


    def test_getting_user_input(self):
        
        root_path = 'testpath'
        self.assertEqual(stock_generator.getting_user_input(), (True, [root_path]))
    

    def test_defining_file_paths(self):

        catalogs_root_path = '../../../Catalogs/Traditional_STR/'

        given_root_path = 'path/test'
        expected_prod_ref_file_path = catalogs_root_path + 'Product_catalog/pebac_ref_prod.xlsx'
        expected_dist_names_file_path = catalogs_root_path + 'dist_names.xlsx'
        expected_product_master_path = catalogs_root_path + 'Product_catalog/product_master.xlsx'
        expected_stock_file_path = 'path/test/stock.txt'

        self.assertEqual(stock_generator.defining_file_paths(given_root_path), 
            (True, [expected_stock_file_path, expected_prod_ref_file_path,
                expected_product_master_path, expected_dist_names_file_path]))


    def test_assigning_stock_to_entrepidus(self):

        df_stock = pd.DataFrame(columns=self.stock_file_columns)
        df_entrepidus_stock = pd.DataFrame(columns=self.stock_entrepidus_columns)

        df_stock['Invoice Date'] = ['20200512']
        df_stock['Product Code'] = ['123']

        expected_df_stock_entrepidus = pd.DataFrame(columns=self.stock_entrepidus_columns)
        expected_df_stock_entrepidus['Date'] = ['20200512']
        expected_df_stock_entrepidus['Store Number'] = ['1stock']
        expected_df_stock_entrepidus['Chain SKU Code'] = ['123']
        expected_df_stock_entrepidus['Unit Sold'] = [0]
        expected_df_stock_entrepidus['Sales Value wotax'] = [0]
        expected_df_stock_entrepidus['Sales Value wtax'] = [0]

        success, content = stock_generator.assigning_stock_to_entrepidus(df_stock, df_entrepidus_stock)
        returned_entrepidus = content[0]

        self.assertEqual(True, success)
        pd.testing.assert_frame_equal(returned_entrepidus, expected_df_stock_entrepidus)
    

    def testing_assigning_dist_names_information(self):

        df_entrepidus_stock = pd.DataFrame(columns=self.stock_entrepidus_columns)
        df_dist_names = pd.DataFrame(columns=self.dist_names_columns)
        expected_df_stock_entrepidus = pd.DataFrame(columns=self.stock_entrepidus_columns)

        df_dist_names['Distributor_country'] = ['Peru']
        df_dist_names['Distributor_id'] = ['288039']
        df_dist_names['Distributor_name'] = ['Jandy']

        df_entrepidus_stock['Diageo_dist_auxiliar_column'] = ['288039']
        df_entrepidus_stock['Distributor'] = ['Jandiii']
        df_entrepidus_stock['Country'] = ['PERIVIS']

        expected_df_stock_entrepidus['Diageo_dist_auxiliar_column'] = ['288039']
        expected_df_stock_entrepidus['Distributor'] = ['Jandy']
        expected_df_stock_entrepidus['Country'] = ['Peru']

        success, content = stock_generator.assigning_dist_names_information(df_entrepidus_stock, df_dist_names)
        returned_df_entrepidus_stock = content[0]

        #Sorting columns to obtain same columns order to both parsed and expected DataFrames
        expected_df_stock_entrepidus = expected_df_stock_entrepidus.sort_index(axis=1, ascending=True)
        returned_df_entrepidus_stock = returned_df_entrepidus_stock.sort_index(axis=1, ascending=True)

        self.assertEqual(success, True)
        pd.testing.assert_frame_equal(returned_df_entrepidus_stock, expected_df_stock_entrepidus, check_names=False)
        

    def test_searching_diageo_sku(self):

        df_pebac_product_reference = pd.DataFrame(columns=self.df_pebac_product_reference_columns)
        df_entrepidus_stock = pd.DataFrame(columns=self.stock_entrepidus_columns)
        expected_df_stock_entrepidus = pd.DataFrame(columns=self.stock_entrepidus_columns)

        df_pebac_product_reference['Dist_Code'] = ['123', '456']
        df_pebac_product_reference['Product_store_id'] = ['444', '777']
        df_pebac_product_reference['Diageo_Sku'] = ['XXX', 'LLL']
        df_pebac_product_reference['Relevant'] = ['Y', 'Y']
        df_pebac_product_reference['Scale'] = [4, 17]

        #Setting indexes to the DataFrame df_pebac_product_reference
        df_pebac_product_reference.set_index(['Dist_Code', 'Product_store_id'], inplace=True) 
        df_pebac_product_reference = df_pebac_product_reference[~df_pebac_product_reference.index.duplicated(keep='first')]

        df_entrepidus_stock['Diageo_dist_auxiliar_column'] = ['123', '456']
        df_entrepidus_stock['Chain SKU Code'] = ['444', '777']

        expected_df_stock_entrepidus['Diageo_dist_auxiliar_column'] = ['123', '456']
        expected_df_stock_entrepidus['Chain SKU Code'] = ['444', '777']
        expected_df_stock_entrepidus['Diageo SKU Code'] = ['XXX', 'LLL']
        expected_df_stock_entrepidus['Aux_product_relevance'] = ['Y', 'Y']

        success, content = stock_generator.searching_diageo_sku(df_entrepidus_stock, df_pebac_product_reference)
        returned_df_entrepidus_stock = content[0]

        #Sorting columns to obtain same columns order to both parsed and expected DataFrames
        expected_df_stock_entrepidus = expected_df_stock_entrepidus.sort_index(axis=1, ascending=True)
        returned_df_entrepidus_stock = returned_df_entrepidus_stock.sort_index(axis=1, ascending=True)

        self.assertEqual(True, success)
        pd.testing.assert_frame_equal(returned_df_entrepidus_stock, expected_df_stock_entrepidus)

    
    def test_filling_product_details(self):

        df_product_master = pd.DataFrame(columns=self.df_product_master_columns)
        df_entrepidus_stock = pd.DataFrame(columns=self.stock_entrepidus_columns)
        expected_df_stock_entrepidus = pd.DataFrame(columns=self.stock_entrepidus_columns)

        update_prod_master = {
            'Material': '999', 
            'Description': 'Test description', 
            'Main Group': 'Cachaça', 
            'Subcategory': 'Pinga', 
            'Brand': 'Vodka',
            'Brand Variant': 'Ardente',
            'Unit Size': 650
        }

        #I am doing this way just to update the DataFrame through a dict
        df_product_master = df_product_master.append(update_prod_master, ignore_index=True)

        df_entrepidus_stock['Diageo SKU Code'] = ['999']

        expected_df_stock_entrepidus['Diageo SKU Code'] = ['999']
        expected_df_stock_entrepidus['Desc Producto & Cód.'] = ['Test description']
        expected_df_stock_entrepidus['Category'] = ['Cachaça']
        expected_df_stock_entrepidus['Sub Category'] = ['Pinga']
        expected_df_stock_entrepidus['Brand'] = ['Vodka']
        expected_df_stock_entrepidus['Brand Variant'] = ['Ardente']
        expected_df_stock_entrepidus['Unit Size'] = 650
        
        success, content = stock_generator.filling_product_details(df_entrepidus_stock, df_product_master)
        returned_df_entrepidus_stock = content[0]

        #Sorting columns to obtain same columns order to both parsed and expected DataFrames
        expected_df_stock_entrepidus = expected_df_stock_entrepidus.sort_index(axis=1, ascending=True)
        returned_df_entrepidus_stock = returned_df_entrepidus_stock.sort_index(axis=1, ascending=True)

        self.assertEqual(True, success)
        pd.testing.assert_frame_equal(returned_df_entrepidus_stock, expected_df_stock_entrepidus, check_dtype=False)
        
    
    def test_calculating_quantity(self):

        df_pebac_product_reference = pd.DataFrame(columns=self.df_pebac_product_reference_columns)
        df_entrepidus_stock = pd.DataFrame(columns=self.stock_entrepidus_columns)
        expected_df_stock_entrepidus = pd.DataFrame(columns=self.stock_entrepidus_columns)

        df_pebac_product_reference['Dist_Code'] = ['123']
        df_pebac_product_reference['Product_store_id'] = ['xxx']
        df_pebac_product_reference['Scale'] = 7.0

        df_entrepidus_stock['Diageo_dist_auxiliar_column'] = ['123']
        df_entrepidus_stock['Chain SKU Code'] = ['xxx']
        df_entrepidus_stock['Unit Sold'] = 3.0

        expected_df_stock_entrepidus['Diageo_dist_auxiliar_column'] = ['123']
        expected_df_stock_entrepidus['Chain SKU Code'] = ['xxx']
        expected_df_stock_entrepidus['Unit Sold'] = 21

        success, content = stock_generator.calculating_quantity(df_entrepidus_stock, df_pebac_product_reference)
        returned_df_entrepidus_stock = content[0]

        #Sorting columns to obtain same columns order to both parsed and expected DataFrames
        expected_df_stock_entrepidus = expected_df_stock_entrepidus.sort_index(axis=1, ascending=True)
        returned_df_entrepidus_stock = returned_df_entrepidus_stock.sort_index(axis=1, ascending=True)

        self.assertEqual(True, success)
        pd.testing.assert_frame_equal(returned_df_entrepidus_stock, expected_df_stock_entrepidus, check_dtype=False)

if __name__ == '__main__':
    unittest.main()