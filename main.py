from struct import Struct
from enum import Enum
from typing import Optional, Tuple
import sys, os

FILE_PATH = "data.bin"
MAXNUMREGS = 11

class Entry:
	format = Struct('> L 20s B')

	def __init__(self, key:int, name:str, age:int):
		self.key = key
		self.name = name
		self.age = age

	@classmethod
	def size(Self) -> int:
		return Self.format.size
	
	@classmethod
	def from_bytes(Self, data:bytes): # -> Entry
		_tuple = Self.format.unpack(data)
		return Self(_tuple[0], str(_tuple[1], 'utf-8').rstrip('\0'), _tuple[2])

	def into_bytes(self) -> bytes:
		return self.format.pack(self.key, bytes(self.name, 'utf-8'), self.age)

	def __str__(self):
		return "[{}] {}, {} anos.".format(self.key, self.name, self.age)

class OpStatus(Enum): 
	OK = 0 
	ERR_KEY_EXISTS = -1
	ERR_OUT_OF_SPACE = -2
	ERR_KEY_NOT_FOUND = -3

class DataBase:
	header_format = Struct('>L')

	def __init__(self, file_path:str):
		self.path = file_path
		try:
			with open(file_path, 'xb') as file:
				self.length = 0
				file.write( self.header_format.pack(self.length) )
		except FileExistsError:
			with open(file_path, "r+b") as file:
				header = file.read(self.header_size())
				_tuple = self.header_format.unpack(header)
				self.length = _tuple[0]

	def __iter__(self):
		self.it_index = -1
		self.it_file = open(self.path, 'rb')
		self.it_file.seek(self.header_size(), 0)
		return self

	def __next__(self):
		self.it_index += 1
		if self.it_index >= self.length:
			self.it_file.close()
			raise StopIteration
		data = self.it_file.read(Entry.size())
		entry = Entry.from_bytes(data)
		return entry

	def __str__(self):
		result = [ 'Local: {} -- Registros: {}'.format(self.path, self.length) ]
		for entry in self:
			result.append( entry.__str__() )
		#result.append('')
		return '\n'.join(result)

	@classmethod
	def header_size(Self) -> int:
		return Self.header_format.size

	@classmethod
	def index_to_ptr(Self, index:int) -> int:
		entry_size = Entry.size()
		header_size = Self.header_size()
		return header_size + index * entry_size

	def set_length(self, length:int) -> None:
		with open(self.path, 'r+b') as file:
			file.seek(0, 0)
			file.write(self.header_format.pack(length))
			self.length = length

	def index_by_key(self, key:int) -> Optional[int]:
		for entry in self:
			if entry.key == key:
				return self.it_index
		return None

	def add_entry(self, key:int, name:str, age:int) -> OpStatus:
		search_result = self.index_by_key(key)
		if search_result is not None:
			return OpStatus.ERR_KEY_EXISTS
		with open(self.path, 'r+b') as file:
			end_pointer = self.index_to_ptr(self.length)
			file.seek(end_pointer)
			file.write( Entry(key, name, age).into_bytes() )
			self.set_length(self.length + 1)
		return OpStatus.OK

	def delete_by_index(self, to_delete:int) -> None:
		with open(self.path, 'r+b') as file:
			for index in range(to_delete, self.length - 1, 1):
				source, dest = self.index_to_ptr(index + 1), self.index_to_ptr(index)
				file.seek(source)
				src_data = file.read(Entry.size())
				file.seek(dest)
				file.write(src_data)
			self.set_length(self.length - 1)

	def delete_by_key(self, key:int) -> OpStatus:
		index = self.index_by_key(key)
		if index is None:
			return OpStatus.ERR_KEY_NOT_FOUND
		self.delete_by_index(index)
		return OpStatus.OK

	def entry_by_index(self, index:int) -> Optional[Entry]:
		with open(self.path, 'rb') as file:
			if index < self.length:
				file.seek(self.header_size() + index * Entry.size(), 0)
				data = file.read(Entry.size())
				entry = Entry.from_bytes(data)
				return entry
			else:
				return None

	def entry_by_key(self, key:int) -> Optional[Entry]:
		with open(self.path, 'rb') as file:
			for i in range(self.length):
				file.seek(self.header_size() + i * Entry.size(), 0)
				data = file.read(Entry.size())
				entry = Entry.from_bytes(data)
				if entry.key == key:
					return entry
			return None

#PRCEDURA

def insert_entry(key:int, name:str, age:int):
	data_base = DataBase(FILE_PATH)
	insert_result = data_base.add_entry(key, name, age)
	if insert_result == OpStatus.OK:
		print('insercao com sucesso: {}'.format(key))
	elif insert_result == OpStatus.ERR_KEY_EXISTS:
		print('chave ja existente: {}'.format(key))
	elif insert_result == OpStatus.ERR_OUT_OF_SPACE:
		print('insercao de chave sem sucesso - arquivo cheio: {}'.format(key))
	else:
		print('DEBUG: erro logico na insercao da chave {}'.format(key))

		
def query_entry(key:int):
	data_base = DataBase(FILE_PATH)
	entry = data_base.entry_by_key(key)
	if entry is not None:
		print(entry)
		#print('chave: {}'.format(entry.key))
		#print(entry.name)
		#print(str(entry.age))
	else:
		print('chave nao encontrada: {}'.format(key))

def remove_entry(key:int):
	data_base = DataBase(FILE_PATH)
	remove_result = data_base.delete_by_key(key)
	if remove_result == OpStatus.OK:
		print('chave removida com sucesso: {}'.format(key))
	elif remove_result == OpStatus.ERR_KEY_NOT_FOUND:	
		print('chave nao encontrada: {}'.format(key))
	else:
		print('DEBUG: erro logico na remocao da chave {}'.format(key))

def print_file():
	data_base = DataBase(FILE_PATH)
	print(data_base)

def benchmark():
	print('BENCHMARK NÃO IMPLEMENTADO')

def exit_shell():
	sys.exit()

database = DataBase(FILE_PATH)
entry = input()
while entry != 'e':
    if(entry == 'i'):
        num_reg = input()
        name_reg = input()
        age_reg = input()
        insert_entry(int(num_reg), name_reg, int(age_reg))
    elif(entry == 'c'):
        num_reg = input()
        database.entry_by_key(int(num_reg))
    elif(entry == 'r'):
        num_reg = input()
        remove_entry(int(num_reg))
    elif(entry == 'p'):
        print_file()
    elif(entry == 'm'):
        print("FALTA IMPLEMENTAR ISSO AINDA VIU!!!!!")
    entry = input()

'''
#TESTE
os.remove(FILE_PATH)#

insert_entry(1, "Abraham Weintraub", 11)
insert_entry(0, "Roberto Carlos", 255)
insert_entry(2, "Pedro Antonhyonhi Silva Costa", 24)
insert_entry(0, "Severino Severo", 44)
insert_entry(4, "João Gabriel", 10)
insert_entry(6, "Fausto Silva", 60)
insert_entry(5, "Pablo Vilar", 19)
print_file()

remove_entry(0)
remove_entry(6)
remove_entry(6)
remove_entry(1)
print_file()

query_entry(0)
query_entry(2)
query_entry(4)
query_entry(6)
print_file()
'''
