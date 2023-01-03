import argparse

f = "/sciclone/geograd/geoBoundaries/scripts/geoBoundaryBot/geoBoundaryBuilder/builderClass.py"
exec(compile(open(f, "rb").read(), f, 'exec'))

parser = argparse.ArgumentParser(description='')
parser.add_argument('iso', type=str,
                    help='ISO of country to build.')
parser.add_argument('adm', type=str,
                    help='ADM of country to build.')
parser.add_argument('product', type=str,
                    help='Product type.')                                        

args = parser.parse_args()
ret = build(ISO=args.iso, ADM=args.adm, product=args.product)
print(ret)