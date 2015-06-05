from swift_cred import sl_storage
import os
import re

def to_send(maindir):
	here = [f for f in os.walk(maindir).next()[2] if f.endswith(".txt")]
	fetchhere = [f for f in os.walk(maindir).next()[2] if f.startswith("fetch")]

	there = []
	there_pre = sl_storage['bart_dump'].objects()

	for pre in there_pre:
		prestr = pre.__str__()
		match = re.search('bart_dump, (.*)\.txt', prestr)
		there.append(match.group(1)+'.txt')

	to_send = list(set(list(set(here) - set(there)) + fetchhere))
	return to_send


def bulk_send(maindir, to_send):
	for item in to_send:
		fullfilename = maindir + '/' + item
		with open(fullfilename, 'r') as readfile:
			sl_storage['bart_dump'][readfile.name.split('/')[-1]].send(readfile)
	print 'Data files sent to object store.'


if __name__ == '__main__':
	bulk_send('data', to_send('data'))