import xml.etree.ElementTree as ET
import pyodbc
import ftplib
def process(arg):
    global user_xml
    user_xml = nested_list[x][0]
    global table
    table = nested_list[x][1]
    global other_table
    other_table= nested_list[x][2]
    global fileName_new
    fileName_new = nested_list[x][3]
    if nested_list[x] == nested_list[x][0]:
         return nested_list[x][0]
    if nested_list[x] == nested_list[x][1]:
         return nested_list[x][1]
    if nested_list[x] == nested_list[x][2]:
         return nested_list[x][2]
    if nested_list[x] == nested_list[x][3]:
         return nested_list[x][3]

    def CreateTable():
        
        hostname = ""
        username = ""
        password = ""
        ftp = ftplib.FTP(hostname,username,password)
        ftp.encoding = "utf-8"

        conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=db-demo-server-123.database.windows.net;DATABASE=XML;UID=db_user;PWD=dataqual123!')
        mytree=ET.parse(user_xml)
        root=mytree.getroot()

        shop = root.find("./SHOPITEM")
        image= root.find(".//IMGURL_ALTERNATIVE")


        def tag():
            def param():
                create_new_table = """
                create table %s(
                product_nums text, param_names text, vals text
                )
                """%(other_table)
                with conn.cursor() as cursor:
                    cursor.execute(create_new_table)
                    conn.commit()

            def param1():
                create_new_table = """
                create table %s(
                product_nums text, param_names text, other_images text
                )
                """%(other_table+"_1")
                with conn.cursor() as cursor:
                    cursor.execute(create_new_table)
                    conn.commit()

            def altimg():
                create_new_table = """
                create table %s(
                item_ids text, alternative_images text
                )
                """%(other_table)
                with conn.cursor() as cursor:
                    cursor.execute(create_new_table)
                    conn.commit()
            
            for x in root.find("./SHOPITEM"):
                if x.tag == "PARAM":
                    return param()
                elif x.tag == "IMGURL_ALTERNATIVE":
                    return altimg()

            for x in root.find("./SHOPITEM"):
                if x.tag == "ADDIMG":
                    return param1()
        tag()
        for x in root.find("./SHOPITEM"):
            if x.tag == "ADDIMG":
                print("{} and {} Completed".format(other_table, other_table+"_1"))
            else:
                print("{} Completed".format(other_table))
        create_new_table_1 = """
        create table %s(
        item_ids text,
        product_nums text,
        product_names text,
        products text,
        descriptions text,
        urls text,
        images text,
        video_urls text,
        prices text,
        price_vats text,
        vats text,
        manufacturers text,
        categories text,
        other_categories text,
        category_levels int,
        eans text,
        heureka_cpcs int,
        delivery_dates int,
        delivery_ids text,
        delivery_prices text,
        delivery_price_codes int,
        param_names text,
        vals text,
        item_groups text,
        availability text,
        sales text,
        accessory text,
        extended_warranty_vals text,
        extended_warranty_descriptions text,
        special_services text,
        sales_voucher_codes text,
        sales_voucher_descriptions text,
        internal_code text
        )
        """%(table)
        with conn.cursor() as cursor:
            cursor.execute(create_new_table_1)
            conn.commit()

    CreateTable()
    print("{} Completed".format(table))
    def CreateXML():
        conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=db-demo-server-123.database.windows.net;DATABASE=XML;UID=db_user;PWD=dataqual123!')

        mytree=ET.parse(user_xml)
        root=mytree.getroot()

        def tag1():
            def param1():
                for x in root.findall('./SHOPITEM'):
                    def product_nums():
                        for y in x:
                            product_nums = x.find('PRODUCTNO')
                            try:
                                return product_nums.text
                            except AttributeError:
                                return " "


                    for param_name in x.findall(".//PARAM"):
                        param_names = param_name.find('PARAM_NAME')
                        vals = param_name.find('VAL')
                        
                        data = """
                        insert into %s(product_nums, param_names, other_images, vals)
                        values(?,?,?)
                        """%(other_table+"_1")
                        with conn.cursor() as cursor:
                            cursor.execute(data, (product_nums(), param_names.text, vals.text))
                            conn.commit()

            def altimg1():
                for x in root.findall('./SHOPITEM'):
                    def item_ids():
                        item_ids = x.find('ITEM_ID')
                        try:
                            return item_ids.text
                        except AttributeError:
                            return ""
                    for image in x.findall(".//IMGURL_ALTERNATIVE"):
                        

                        data = """
                        insert into %s(item_ids, alternative_images)
                        values(?,?)
                        """%(other_table)
                        with conn.cursor() as cursor:
                            cursor.execute(data, (item_ids(), image.text))
                            conn.commit()

            for x in root.find("./SHOPITEM"):
                if x.tag == "PARAM":
                    return param1()
                elif x.tag == "IMGURL_ALTERNATIVE":
                    return altimg1()


        tag1()
        print("{} Uploaded".format(other_table))
        def tag2():
            def param2():
                for x in root.findall('./SHOPITEM'):
                    def product_nums():
                        for y in x:
                            product_nums = x.find('PRODUCTNO')
                            try:
                                return product_nums.text
                            except AttributeError:
                                return " "
                        

                    for image in root.findall(".//ADDIMG"):
                        images = image.find('IMGURL')
                        
                        data = """
                        insert into %s(product_nums, param_names, other_images)
                        values(?,?,?)
                        """%(other_table)
                        with conn.cursor() as cursor:
                            cursor.execute(data, (product_nums(), param_names.text, images.text))
                            conn.commit()

            for x in root.find("./SHOPITEM"):
                if x.tag == "ADDIMG":
                    return param2()
        tag2()
        print("{} Uploaded".format(other_table+"_1"))  

        mytree=ET.parse(user_xml)
        root=mytree.getroot()

        for x in root.find('./SHOPITEM'):
            if x.tag == "ITEM_ID":
                for x in root.findall('./SHOPITEM'):
                    def item_ids():
                        item_ids = x.find('ITEM_ID')
                        try:
                            return item_ids.text
                        except AttributeError:
                            return ""
                        
            if x.tag == "PRODUCTNO":
                for x in root.findall('./SHOPITEM'):
                    def product_nums():
                        for y in x:
                            product_nums = x.find('PRODUCTNO')
                            try:
                                return product_nums.text
                            except AttributeError:
                                return " "

            if x.tag == "PRODUCTNAME":
                for x in root.findall('./SHOPITEM'):
                    def product_names():
                        product_names = x.find('PRODUCTNAME')
                        try:
                            return product_names.text
                        except AttributeError:
                            return ""

            if x.tag == "PRODUCT":
                for x in root.findall('./SHOPITEM'):
                    def products():
                        products = x.find('PRODUCT')
                        try:
                            return products.text
                        except AttributeError:
                            return ""

            if x.tag == "DESCRIPTION":
                for x in root.findall('./SHOPITEM'):
                    def descriptions():
                        descriptions = x.find('DESCRIPTION')
                        try:
                            return descriptions.text
                        except AttributeError:
                            return ""

            if x.tag == "URL":
                for x in root.findall('./SHOPITEM'):
                    def urls():
                        urls = x.find('URL')
                        try:
                            return urls.text
                        except AttributeError:
                            return ""
            if x.tag == "IMGURL":
                for x in root.findall('./SHOPITEM'):
                    def images():
                        images = x.find('IMGURL')
                        try:
                            return images.text
                        except AttributeError:
                            return ""


            if x.tag == "PRICE":
                for x in root.findall('./SHOPITEM'):
                    def prices():
                        prices = x.find('PRICE')
                        try:
                            return prices.text
                        except AttributeError:
                            return ""

            if x.tag == "VIDEO_URL":
                for x in root.findall('./SHOPITEM'):
                    def video_urls():
                        video_urls = x.find('VIDEO_URL')
                        try:
                            return video_urls.text
                        except AttributeError:
                            return ""

            if x.tag == "PRICE_VAT":
                for x in root.findall('./SHOPITEM'):
                    def price_vats():
                        price_vats = x.find('PRICE_VAT')
                        try:
                            return price_vats.text
                        except AttributeError:
                            return ""

            if x.tag == "VAT":
                for x in root.findall('./SHOPITEM'):
                    def vats():
                        vats = x.find('VAT')
                        try:
                            return vats.text
                        except AttributeError:
                            return ""

            if x.tag == "MANUFACTURER":
                for x in root.findall('./SHOPITEM'):
                    def manufacturers():
                        manufacturers = x.find('MANUFACTURER')
                        try:
                            return manufacturers.text
                        except AttributeError:
                            return ""

            if x.tag == "CATEGORYTEXT":
                for x in root.findall("./SHOPITEM"):
                    def categories():
                        categories = x.find('CATEGORYTEXT')
                        try:
                            return categories.text
                        except AttributeError:
                            return ""

            if x.tag == "CATEGORIES":
                for x in root.findall("./SHOPITEM"):
                    def categories_1():
                        for category in root.findall(".//CATEGORIES"):
                            categories = category.find('CATEGORY_NAME')
                            try:
                                return categories.text
                            except AttributeError:
                                return ""
                            
                    def categories_level():
                        for category in root.findall(".//CATEGORIES"):
                            categories = category.find('CATEGORY_LEVEL')
                            try:
                                return categories.text
                            except AttributeError:
                                return ""

            if x.tag == "HEUREKA_CPC":
                for x in root.findall("./SHOPITEM"):
                    def heureka_cpcs():
                        for y in x:
                            heureka_cpcs = x.find('HEUREKA_CPC')
                            try:
                                return heureka_cpcs.text
                            except AttributeError:
                                return ""

            if x.tag == "DELIVERY_DATE":
                for x in root.findall("./SHOPITEM"):
                    def delivery_dates():
                        delivery_dates = x.find('DELIVERY_DATE')
                        try:
                            return delivery_dates.text
                        except AttributeError:
                            return ""

            if x.tag == "EAN":
                for x in root.findall("./SHOPITEM"):
                    def eans():
                        eans = x.find('EAN')
                        try:
                            return eans.text
                        except AttributeError:
                            return ""

            if x.tag == "DELIVERY":
                for x in root.findall("./SHOPITEM"):
                    def delivery_ids():
                        for info in root.findall('.//DELIVERY'):
                            id_infos = info.find('DELIVERY_ID')
                            try:
                                return id_infos.text
                            except AttributeError:
                                return ""
                    def delivery_prices():
                        for info in root.findall('.//DELIVERY'):
                            prices = info.find('DELIVERY_PRICE')
                            try:
                                return prices.text
                            except AttributeError:
                                return ""
                    def delivery_price_codes():
                        for info in root.findall('.//DELIVERY'):
                            price_codes = info.find('DELIVERY_PRICE_COD')
                            try:
                                return price_codes.text
                            except AttributeError:
                                return ""

            if x.tag == "PARAM":
                for x in root.findall("./SHOPITEM"):
                    def param_names():
                        for param_name in root.findall(".//PARAM"):
                            param_names = param_name.find('PARAM_NAME')
                            try:
                                return param_names.text
                            except AttributeError:
                                return ""
                        
                    def vals():
                        vals = x.find('VAL')
                        try:
                            return vals.text
                        except AttributeError:
                            return ""

                    def vals_1():
                        for val in root.findall(".//PARAM"):
                            vals = val.find('VAL')
                            try:
                               return vals.text
                            except AttributeError:
                                return ""

            if x.tag == "ITEMGROUP_ID":
                for x in root.findall("./SHOPITEM"):
                    def item_groups():
                        item_groups = x.find('ITEMGROUP_ID')
                        try:
                            return item_groups.text
                        except AttributeError:
                            return ""

            if x.tag == "AVAILABILITY":
                for x in root.findall("./SHOPITEM"):
                    def avails():
                        avails = x.find('AVAILABILITY')
                        try:
                            return avails.text
                        except AttributeError:
                            return ""

            if x.tag == "SALE":
                for x in root.findall("./SHOPITEM"):
                    def sales():
                        sales = x.find('SALE')
                        try:
                            return sales.text
                        except AttributeError:
                            return ""

            if x.tag == "ACCESSORY":
                for x in root.findall("./SHOPITEM"):
                    def access():
                        access = x.find('ACCESSORY')
                        try:
                            return access.text
                        except AttributeError:
                            return ""

            if x.tag == "EXTENDED_WARRANTY":
                for x in root.findall("./SHOPITEM"):
                    def extendwar_val():
                        for ext in root.findall(".//EXTENDED_WARRANTY"):
                            exts = ext.find('VAL')
                            try:
                                return exts.text
                            except AttributeError:
                                return ""
                        
                    def extendwar_desc():
                        for ext in root.findall(".//EXTENDED_WARRANTY"):
                            exts = ext.find('DESC')
                            try:
                                return exts.text
                            except AttributeError:
                                return ""
                
            if x.tag == "SPECIAL_SERVICE":
                for x in root.findall("./SHOPITEM"):
                    def special_serv():
                        special_serv = x.findall('SPECIAL_SERVICE')
                        try:
                            return special_serv.text
                        except AttributeError:
                                return ""
                
            if x.tag == "SALES_VOUCHER":
                for x in root.findall("./SHOPITEM"):
                    def salesvouch_cod():
                        for sale in root.findall(".//SALES_VOUCHER"):
                            cod = sale.find('CODE')
                            try:
                                return cod.text
                            except AttributeError:
                                return ""

                    def salesvouch_desc():
                        for sale in root.findall(".//SALES_VOUCHER"):
                            desc = sale.find('DESC')
                            try:
                                return desc.text
                            except AttributeError:
                                return ""

            if x.tag == "INTERNALCODEMO":
                for x in root.findall("./SHOPITEM"):
                    def internal_codes():
                        internal_code = x.find("INTERNALCODEMO")
                        try:
                            return internal_code.text
                        except AttributeError:
                            return ""
            
            data = """
            insert into %s(item_ids, product_nums, product_names, products, descriptions, urls, images, video_urls, prices, price_vats, vats, manufacturers, categories, other_categories, category_levels, eans, heureka_cpcs, delivery_dates, delivery_ids, delivery_prices, delivery_price_codes, param_names, vals, item_groups, availability, sales, accessory, extended_warranty_vals, extended_warranty_descriptions, special_services, sales_voucher_codes, sales_voucher_descriptions, internal_code)
            values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """%(table)
            with conn.cursor() as cursor:
                cursor.execute(data, (item_ids(), product_nums(), product_names(), products(), descriptions(), urls(), images(), video_urls(), prices(), price_vats(), vats(), manufacturers(),categories(), categories_1(), categories_level(), eans(), heureka_cpcs(), delivery_dates(), delivery_ids(), delivery_prices(), delivery_price_codes(), param_names(), vals_1(), item_groups(), avails(), sales(), access(), extendwar_val(), extendwar_desc(), special_serv(), salesvouch_cod(), salesvouch_desc(), internal_codes()))
                conn.commit()

    CreateXML()
    print("{} Uploaded".format(table))

    def NewXML():
        

        conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=;DATABASE=XML;UID=;PWD=')
        mytree=ET.parse(user_xml)
        root=mytree.getroot()
        def GetItemID():
            cursor = conn.cursor()
            SQL = "select item_ids from %s"%(table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall()        
                list=[]
                for x in rows:
                    list.append("".join(x))
                return list

        def GetItem1():
            cursor=conn.cursor()
            SQL="select item_ids,images from %s"%(table)
            with conn.cursor() as cursor:
                cursor.execute(SQL)
                rows=cursor.fetchall()
                return rows

        def GetProNum():
            cursor = conn.cursor()
            SQL = "select product_nums from %s"%(table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall()
                list=[]
                for x in rows:
                    list.append("".join(x))
                return list


        def GetProNam():
            cursor = conn.cursor()
            SQL = "select product_names from %s"%(table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    list.append("".join(x))
                return list

        def GetProduct():
            cursor = conn.cursor()
            SQL = "select products from %s"%(table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    try:
                        return list.append("".join(x))
                    except TypeError:
                        return " "
                return list

        def GetDesc():
            cursor = conn.cursor()
            SQL = "select descriptions from %s"%(table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    list.append("".join(x))
                return list
            
        def GetCat1():
            cursor = conn.cursor()
            SQL = "select categories from %s"%(table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    list.append("".join(x))
                return list

        def GetUrl():
            cursor = conn.cursor()
            SQL = "select urls from %s"%(table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    list.append("".join(x))
                return list

        def GetImage():
            cursor = conn.cursor()
            SQL = "select images from %s"%(table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    list.append("".join(x))
                return list

        def GetAlt():
            cursor = conn.cursor()
            SQL = "select item_ids, alternative_images from %s"%(other_table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                return rows

        def GetPrice1():
            cursor = conn.cursor()
            SQL = "select prices from %s"%(table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    try:
                        return list.append("".join(x))
                    except TypeError:
                        return " "
                return list

        def GetVid():
            cursor = conn.cursor()
            SQL = "select video_urls from %s"%(table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    try:
                        return list.append("".join(x))
                    except TypeError:
                        return " "
                return list


        def GetPrice():
            cursor = conn.cursor()
            SQL = "select price_vats from %s"%(table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    list.append(x[0])
                return list

        def GetVat():
            cursor = conn.cursor()
            SQL = "select vats from %s"%(table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    list.append("".join(x))
                return list

        def GetMan():
            cursor = conn.cursor()
            SQL = "select manufacturers from %s"%(table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    list.append("".join(x))
                return list

        def GetCat2():
            cursor = conn.cursor()
            SQL = "select other_categories from %s"%(table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    try:
                        list.append("".join(x))
                    except TypeError:
                        return ""
                return list

        def GetCatlevel():
            cursor = conn.cursor()
            SQL = "select category_levels from %s"%(table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    list.append("".join(str(x[0])))
                return list



        def GetEAN():
            cursor = conn.cursor()
            SQL = "select eans from %s"%(table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    list.append("".join(x))
                return list

        def GetCPC():
            cursor = conn.cursor()
            SQL = "select heureka_cpcs from %s"%(table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    list.append(x[0])
                return list

        def GetDelDat():
            cursor = conn.cursor()
            SQL = "select delivery_dates from %s"%(table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    list.append(x[0])
                return list
            
        def GetDelID():
            cursor = conn.cursor()
            SQL = "select delivery_ids from %s"%(table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    list.append("".join(x))
                return list

        def GetDelP():
            cursor = conn.cursor()
            SQL = "select delivery_prices from %s"%(table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    list.append(x[0])
                return list


        def GetDelC():
            cursor = conn.cursor()
            SQL = "select delivery_price_codes from %s"%(table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    list.append(str(x[0]))
                return list

        def GetParamNam():
            cursor = conn.cursor()
            SQL = "select param_names from %s"%(table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    try:
                        return list.append("".join(str(x)))
                    except TypeError:
                        return " "
                return list

        def GetVals1():
            cursor = conn.cursor()
            SQL = "select vals from %s"%(table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    try:
                        return list.append("".join(str(x)))
                    except TypeError:
                        return " "
                return list

        def GetVals2():
            cursor = conn.cursor()
            SQL = "select vals from %s"%(other_table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    list.append("".join(x))
                return list

        def GetItemGroups():
            cursor = conn.cursor()
            SQL = "select item_groups from %s"%(table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    list.append("".join(str(x)))
                return list

        def GetAvails():
            cursor = conn.cursor()
            SQL = "select availability from %s"%(table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    list.append("".join(str(x[0])))
                return list

        def GetSales():
            cursor = conn.cursor()
            SQL = "select sales from %s"%(table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    list.append("".join(str(x[0])))
                return list

        def GetAccess():
            cursor = conn.cursor()
            SQL = "select accessory from %s"%(table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    list.append("".join(str(x)))
                return list

        def GetExtVals():
            cursor = conn.cursor()
            SQL = "select extended_warranty_vals from %s"%(table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    list.append("".join(str(x)))
                return list

        def GetExtDesc():
            cursor = conn.cursor()
            SQL = "select extended_warranty_descriptions from %s"%(table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    list.append("".join(str(x)))
                return list

        def GetSpecialServs():
            cursor = conn.cursor()
            SQL = "select special_services from %s"%(table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    list.append("".join(str(x)))
                return list

        def GetSalesVouchCod():
            cursor = conn.cursor()
            SQL = "select sales_voucher_codes from %s"%(table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    list.append("".join(str(x)))
                return list

        def GetSalesVouchDescs():
            cursor = conn.cursor()
            SQL = "select sales_voucher_descriptions from %s"%(table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    list.append("".join(str(x)))
                return list

        def GetInternalCodes():
            cursor = conn.cursor()
            SQL = "select internal_code from %s"%(table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    list.append("".join(str(x)))
                return list


        def CreateXML(fileName):
            root=ET.Element("Shop")
            for item in GetItemID():
                c0=ET.Element("ShopItem")
                root.append(c0)
                c1=ET.Element("ItemInfo")
                c0.append(c1)
                c2=ET.Element("Url")
                c0.append(c2)
                c3=ET.Element("Image")
                c0.append(c3)
                c4=ET.Element("ProductInfo")
                c0.append(c4)
                c5=ET.Element("Delivery")
                c0.append(c5)


            def InputItemID():            
                list1 = GetItemID()
                x=0
                for elm in root.findall(".//ItemInfo"):
                    ET.SubElement(elm, "ItemID").text= list1[x]
                    x+=1
            
            def InputProNum():
                list2 = GetProNum()
                x=0
                for elm in root.findall(".//ItemInfo"):
                    ET.SubElement(elm, "ProductNum").text= list2[x]
                    x+=1
            
            def InputProNam():
                list3 = GetProNam()
                x=0
                for elm in root.findall(".//ItemInfo"):
                    ET.SubElement(elm, "ProductName").text= list3[x]
                    x+=1
            
            def InputDesc():
                list4 = GetDesc()
                x=0
                for elm in root.findall(".//ItemInfo"):
                    ET.SubElement(elm, "Description").text= list4[x]
                    x+=1

            def InputItemGroup():
                list20 = GetItemGroups()
                x=0
                for elm in root.findall(".//ItemInfo"):
                    ET.SubElement(elm, "Item_Group").text= str(list20[x])
                    x+=1

            def InputProducts():
                list30 = GetProduct()
                x=0
                for elm in root.findall(".//ItemInfo"):
                    ET.SubElement(elm, "Product").text= list30[x]
                    x+=1

            def InputInternalCodes():
                list31 = GetInternalCodes()
                x=0
                for elm in root.findall(".//ItemInfo"):
                    ET.SubElement(elm, "Internal_Code").text= list31[x]
                    x+=1


            def ItemInfo():
                mytree=ET.parse(user_xml)
                root1=mytree.getroot()            
                for x in root1.find("./SHOPITEM"):
                    if x.tag == "ITEM_ID":
                        InputItemID()
                    if x.tag == "PRODUCTNO":
                        InputProNum()                
                    if x.tag == "DESCRIPTION":
                        return InputDesc()
                    if x.tag == "PRODUCTNAME":
                        InputProNam()
                    if x.tag == "ITEMGROUP_ID":
                        InputItemGroup()
                    if x.tag == "PRODUCT":
                        InputProducts()
                    if x.tag == "INTERNALCODEMO":
                        InputInternalCodes()


            ItemInfo()

            def InputUrl():
                list6 = GetUrl()
                x=0
                for elm in root.findall(".//Url"):
                    ET.SubElement(elm, "Url").text= list6[x]
                    x+=1
            def URL():
                mytree=ET.parse(user_xml)
                root1=mytree.getroot()            
                for x in root1.find("./SHOPITEM"):
                    if x.tag == "URL":
                        InputUrl()
            URL()
            
            def InputImage():
                list7 = GetImage()
                x=0
                for elm in root.findall(".//Image"):
                    ET.SubElement(elm, "FirstImg").text= list7[x]
                    x+=1
            def Images():
                mytree=ET.parse(user_xml)
                root1=mytree.getroot()            
                for x in root1.find("./SHOPITEM"):
                    if x.tag == "IMGURL":
                        InputImage()
            Images()

            def InputPrice():
                list9 = GetPrice1()
                x=0
                for elm in root.findall(".//ProductInfo"):
                    ET.SubElement(elm, "Price").text= str(list9[x])
                    x+=1
            

            def InputPriceVat():
                list9 = GetPrice()
                x=0
                for elm in root.findall(".//ProductInfo"):
                    ET.SubElement(elm, "PriceVat").text= str(list9[x])
                    x+=1
            
            def InputVat():
                list10= GetVat()
                x=0
                for elm in root.findall(".//ProductInfo"):
                    ET.SubElement(elm, "Vat").text= list10[x]
                    x+=1
            
            def InputMan():
                list11= GetMan()
                x=0
                for elm in root.findall(".//ProductInfo"):
                    ET.SubElement(elm, "Manufacturer").text= list11[x]
                    x+=1
            
            def InputCat1():
                list12 = GetCat1()
                x=0
                for elm in root.findall(".//ProductInfo"):
                    ET.SubElement(elm, "Category").text= list12[x]
                    x+=1
            
            def InputCat2():
                list13 = GetCat2()
                x=0
                for elm in root.findall(".//ProductInfo"):
                    ET.SubElement(elm, "Other_Category").text= list13[x]
                    x+=1

            def InputCatlevel():
                list29 = GetCatlevel()
                x=0
                for elm in root.findall(".//ProductInfo"):
                    ET.SubElement(elm, "Category_Level").text= list29[x]
                    x+=1


            def InputEAN():
                list14 = GetEAN()
                x=0
                for elm in root.findall(".//ProductInfo"):
                    ET.SubElement(elm, "EAN").text= list14[x]
                    x+=1
            
            def InputCPC():
                list15 = GetCPC()
                x=0
                for elm in root.findall(".//ProductInfo"):
                    ET.SubElement(elm, "HeurekaCPC").text= str(list15[x])
                    x+=1
            def InputAvail():
                list21 = GetAvails()
                x=0
                for elm in root.findall(".//ProductInfo"):
                    ET.SubElement(elm, "Availability").text= str(list21[x])
                    x+=1
            
            def InputSale():
                list22 = GetSales()
                x=0
                for elm in root.findall(".//ProductInfo"):
                    ET.SubElement(elm, "Sale").text= str(list22[x])
                    x+=1
            
            def InputAccess():
                list23 = GetAccess()
                x=0
                for elm in root.findall(".//ProductInfo"):
                    ET.SubElement(elm, "Accessory").text= str(list23[x])
                    x+=1
            
            def InputExtVal():
                list24 = GetExtVals()
                x=0
                for elm in root.findall(".//ProductInfo"):
                    ET.SubElement(elm, "Extended_Warranty_Value").text= str(list24[x])
                    x+=1
            
            def InputExtDesc():
                list25 = GetExtDesc()
                x=0
                for elm in root.findall(".//ProductInfo"):
                    ET.SubElement(elm, "Extended_Warranty_Description").text= str(list25[x])
                    x+=1
            
            def InputSpecialServ():
                list26 = GetSpecialServs()
                x=0
                for elm in root.findall(".//ProductInfo"):
                    ET.SubElement(elm, "Special_Service").text= str(list26[x])
                    x+=1
            
            def InputSaleVouchCod():
                list27 = GetSalesVouchCod()
                x=0
                for elm in root.findall(".//ProductInfo"):
                    ET.SubElement(elm, "Sale_Voucher_Code").text= str(list27[x])
                    x+=1
            
            def InputSaleVouchDesc():
                list28 = GetSalesVouchDescs()
                x=0
                for elm in root.findall(".//ProductInfo"):
                    ET.SubElement(elm, "Sale_Voucher_Description").text= str(list28[x])
                    x+=1

                    
            def ProductInfo():
                mytree=ET.parse(user_xml)
                root1=mytree.getroot()            
                for x in root1.find("./SHOPITEM"):
                    if x.tag == "SALES_VOUCHER":
                        Items(InputSaleVouchDesc())
                    if x.tag == "SALES_VOUCHER":
                        InputSaleVouchCod()
                    if x.tag == "SPECIAL_SERVICE":
                        InputSpecialServ()
                    if x.tag == "EXTENDED_WARRANTY":
                        InputExtDesc()
                    if x.tag == "EXTENDED_WARRANTY":
                        InputExtVal()
                    if x.tag == "ACCESSORY":
                        InputAccess()
                    if x.tag == "SALE":
                        InputSale()
                    if x.tag == "AVAILABILITY":
                        InputAvail()
                    if x.tag == "HEUREKA_CPC":
                        InputCPC()
                    if x.tag == "EAN":
                        InputEAN()
                    if x.tag == "CATEGORYTEXT":
                        InputCat1()
                    if x.tag == "CATEGORIES":
                        InputCat2()
                    if x.tag == "CATEGORIES":
                        InputCatlevel()
                    if x.tag == "MANUFACTURER":
                        InputMan()
                    if x.tag == "VAT":
                        InputVat()
                    if x.tag == "PRICE_VAT":
                        InputPriceVat()
                    if x.tag == "PRICE":
                        InputPrice()

            ProductInfo()
            
            def InputDelDat():
                list16 = GetDelDat()
                x=0
                for elm in root.findall(".//Delivery"):
                    ET.SubElement(elm, "DeliveryDate").text= str(list16[x])
                    x+=1
            
            def InputDelID():
                list17 = GetDelID()
                x=0
                for elm in root.findall(".//Delivery"):
                    ET.SubElement(elm, "DeliveryID").text= list17[x]
                    x+=1
            
            def InputDelP():
                list18 = GetDelP()
                x=0
                for elm in root.findall(".//Delivery"):
                    ET.SubElement(elm, "DeliveryPrice").text= str(list18[x])
                    x+=1
            
            def InputDelC():
                list19 = GetDelC()
                x=0
                for elm in root.findall(".//Delivery"):
                    ET.SubElement(elm, "DeliveryPriceCOD").text= str(list19[x])
                    x+=1

            def Deliveries():
                mytree=ET.parse(user_xml)
                root1=mytree.getroot()            
                for x in root1.find("./SHOPITEM"):
                    if x.tag == "DELIVERY":
                        InputDelC()
                    if x.tag == "DELIVERY":
                        InputDelP()
                    if x.tag == "DELIVERY":
                        InputDelID()
                    if x.tag == "DELIVERY_DATE":
                        InputDelDat()
            Deliveries()       

                            
            tree=ET.ElementTree(root)
            with open(fileName, "wb") as files:
                tree.write(files)

        if __name__=="__main__":
            CreateXML(fileName_new)


    NewXML()
    print("{} almost Completed".format(fileName_new))

    def tag3():
        conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=;DATABASE=XML;UID=;PWD=')

        def GetItemID():
            cursor = conn.cursor()
            SQL = "select item_ids from %s"%(table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall()        
                list=[]
                for x in rows:
                    list.append("".join(x))
                return list

        def GetItem1():
            cursor=conn.cursor()
            SQL="select item_ids,images from %s"%(table)
            with conn.cursor() as cursor:
                cursor.execute(SQL)
                rows=cursor.fetchall()
                return rows

        def GetProNum():
            cursor = conn.cursor()
            SQL = "select product_nums from %s"%(table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall()
                list=[]
                for x in rows:
                    list.append("".join(x))
                return list


        def GetProNam():
            cursor = conn.cursor()
            SQL = "select product_names from %s"%(table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    list.append("".join(x))
                return list

        def GetProduct():
            cursor = conn.cursor()
            SQL = "select products from %s"%(table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    list.append("".join(x))
                return list

        def GetDesc():
            cursor = conn.cursor()
            SQL = "select descriptions from %s"%(table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    list.append("".join(x))
                return list
            
        def GetCat1():
            cursor = conn.cursor()
            SQL = "select categories from %s"%(table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    list.append("".join(x))
                return list

        def GetUrl():
            cursor = conn.cursor()
            SQL = "select urls from %s"%(table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    list.append("".join(x))
                return list

        def GetImage():
            cursor = conn.cursor()
            SQL = "select images from %s"%(table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    list.append("".join(x))
                return list

        def GetOtherImg():
            cursor = conn.cursor()
            SQL = "select other_images from %s"%(other_table+"_1")
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    list.append("".join(x))
                return list


        def GetAlt():
            cursor = conn.cursor()
            SQL = "select item_ids, alternative_images from %s"%(other_table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                return rows

        def GetPrice1():
            cursor = conn.cursor()
            SQL = "select prices from %s"%(table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    list.append("".join(x))
                return list

        def GetVid():
            cursor = conn.cursor()
            SQL = "select video_urls from %s"%(table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    list.append("".join(x))
                return list


        def GetPrice():
            cursor = conn.cursor()
            SQL = "select price_vats from %s"%(table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    list.append(x[0])
                return list

        def GetVat():
            cursor = conn.cursor()
            SQL = "select vats from %s"%(table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    list.append("".join(x))
                return list

        def GetMan():
            cursor = conn.cursor()
            SQL = "select manufacturers from %s"%(table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    list.append("".join(x))
                return list

        def GetCat2():
            cursor = conn.cursor()
            SQL = "select other_categories from %s where other_categories is not null"%(table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    try:
                        list.append("".join(x))
                    except TypeError:
                        return ""
                return list

        def GetEAN():
            cursor = conn.cursor()
            SQL = "select eans from %s"%(table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    list.append("".join(x))
                return list

        def GetCPC():
            cursor = conn.cursor()
            SQL = "select heureka_cpcs from %s"%(table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    list.append(x[0])
                return list

        def GetDelDat():
            cursor = conn.cursor()
            SQL = "select delivery_dates from %s"%(table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    list.append(x[0])
                return list
            
        def GetDelID():
            cursor = conn.cursor()
            SQL = "select delivery_ids from %s"%(table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    list.append("".join(x))
                return list

        def GetDelP():
            cursor = conn.cursor()
            SQL = "select delivery_prices from %s"%(table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    list.append(x[0])
                return list

        def GetDelC():
            cursor = conn.cursor()
            SQL = "select delivery_price_codes from %s"%(table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    list.append(x[0])
                return list

        def GetParamNam():
            cursor = conn.cursor()
            SQL = "select product_nums , param_names, vals from %s"%(other_table)
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    list.append(x)
                return list


        def GetVals1():
            cursor = conn.cursor()
            SQL = "select vals from %s"%(table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    list.append("".join(x))
                return list

        def GetVals2():
            cursor = conn.cursor()
            SQL = "select param_names, vals from %s"%(other_table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    list.append(x)
                return list

        def GetItemGroups():
            cursor = conn.cursor()
            SQL = "select item_groups from %s"%(table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    list.append("".join(x))
                return list

        def GetAvails():
            cursor = conn.cursor()
            SQL = "select availability from %s"%(table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    list.append("".join(x))
                return list

        def GetSales():
            cursor = conn.cursor()
            SQL = "select sales from %s"%(table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    list.append("".join(x))
                return list

        def GetAccess():
            cursor = conn.cursor()
            SQL = "select accesses from %s"%(table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    list.append("".join(x))
                return list

        def GetExtVals():
            cursor = conn.cursor()
            SQL = "select extended_warranty_vals from %s"%(table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    list.append("".join(str(x)))
                return list

        def GetExtDesc():
            cursor = conn.cursor()
            SQL = "select extended_warranty_descriptions from %s"%(table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    list.append("".join(str(x)))
                return list

        def GetSpecialServs():
            cursor = conn.cursor()
            SQL = "select special_services from %s"%(table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    list.append("".join(x))
                return list

        def GetSalesVouchCod():
            cursor = conn.cursor()
            SQL = "select sales_voucher_codes from %s"%(table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    list.append("".join(str(x)))
                return list

        def GetSalesVouchDescs():
            cursor = conn.cursor()
            SQL = "select sales_voucher_descriptions from %s"%(table)
            
            with conn.cursor() as cursor:         
                cursor.execute(SQL)
                rows = cursor.fetchall();
                list=[]
                for x in rows:
                    list.append("".join(str(x)))
                return list

        def InputParam():
            tree = ET.parse(fileName_new)
            root = tree.getroot()

            def ParamNam():
                new_list= []
                list1= []
                for a in GetProNum():
                    if a not in list1:
                        list1.append(a)
                for b in GetParamNam():
                    if b not in new_list:
                        new_list.append(b)


                        
                for elm in root.findall(".//ItemInfo"):
                    images = elm.find(".//ProductNum")
                    for z in new_list:
                        if z[0] == images.text:
                                ET.SubElement(elm, "Param_Name").text=z[1]
                                ET.SubElement(elm, "Param_Value").text=z[2]                            

            tree.write(fileName_new)

        def InputAltImg():
            tree = ET.parse(fileName_new)
            root = tree.getroot()

            new_list= []
            list1= []
            for a in GetItem1():
                if a not in list1:
                    list1.append(a)
            for b in GetAlt():
                if b not in new_list:
                    new_list.append(b)

            res = list(map(list,list1))

            for x in new_list:
                for y in res:
                    if x[0] == y[0]:
                        if x[1] not in y:
                            y.insert(2,x[1])

            list2 = list(map(tuple, res))

            for elm in root.findall(".//Image"):
                images = elm.find(".//FirstImg")
                for z in list2:
                    if z[1] == images.text:
                        for y in z[2:]:
                            ET.SubElement(elm, "AltImg").text=y


            tree.write(fileName_new)        

            
        mytree=ET.parse(user_xml)
        root=mytree.getroot()

        for x in root.find("./SHOPITEM"):
            if x.tag == "PARAM":
                return InputParam()
            elif x.tag == "IMGURL_ALTERNATIVE":
                return InputAltImg()

        tag3()
        def tag4():
            tree = ET.parse(fileName_new)
            root = tree.getroot()

            def InputImg():
                new_list= []
                list1= []
                for a in GetProNum():
                    if a not in list1:
                        list1.append(a)
                for b in GetOtherImg():
                    if b not in new_list:
                        new_list.append(b)


                        
                for elm in root.findall(".//ItemInfo"):
                    images = elm.find(".//Image")
                    for z in new_list:
                        if z[0] == images.text:
                                ET.SubElement(elm, "SecondImg").text=z[1]

                tree.write(fileName_new)        

            for x in root.find("./SHOPITEM"):
                if x.tag == "ADDIMG":
                    return InputImg()
        tag4()
        print("{} Completed".format(fileName_new))


def userinput():
    global user_xml_input
    user_xml_input = []
    global table_input
    table_input=[]
    global other_table_input
    other_table_input=[]
    global fileName_new_input
    fileName_new_input=[]
    global list_of_inputs
    list_of_inputs = [user_xml_input, table_input, other_table_input, fileName_new_input]

    while True:
        user_question = input("Put in xml, table and new file (Y/N): ")
        user_question = user_question[0].upper()
        if user_question == "Y":
            first = input("Enter XML: ")
            user_xml_input.append(first)
            second=input("Table Name: ")
            table_input.append(second)
            third=input("Other Table Name: ")
            other_table_input.append(third)
            fourth = input("Enter New XML Name: ")
            fileName_new_input.append(fourth)
        elif user_question == "N":
            break
    return user_xml_input, table_input, other_table_input, fileName_new_input

userinput()
nested_list = [[] for _ in range(len(user_xml_input))]
for x in range(len(user_xml_input)):
    for y in list_of_inputs:
        if y[x] not in nested_list[x]:
            nested_list[x].append(y[x])

for x in range(len(user_xml_input)):
    print(process(nested_list[x]))            



