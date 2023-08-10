import jqdatasdk

jqdatasdk.auth("13560795956", "Gonfu12/")

df = jqdatasdk.get_price('000001.XSHE')
print(df)