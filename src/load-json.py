from apim_types import *
from apim_clients import *
from urlparse import urlparse
import datetime
import base64
import sys
import os, os.path
import codecs
import hashlib
import shutil
import json
import getopt

def loadTemplate(fname):
	t = codecs.open(os.path.join('.', fname), 'rU','utf-8')
	r = t.read()
	t.close()
	return r
def testUri(test):
  return str(test).startswith('info:fedora/') or urlparse(test)[0] in ['http','https','file']

STUB_TEMPLATE = loadTemplate(sys.path[0] + '/object-stub.xml')
FMODEL = 'info:fedora/fedora-system:def/model#'
DC_TEMPLATE = U"""<?xml version="1.0" encoding="UTF-8"?><oai_dc:dc xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/" \
xmlns:dc="http://purl.org/dc/elements/1.1/" \
xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" \
xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/oai_dc/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd">{0[dc]}</oai_dc:dc>"""
RELS_TEMPLATE = U"""<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#">{0[rdf]}</rdf:RDF>"""
class FedoraServices(object):
  def __init__(self,debug=False):
    self.map = dict()
    self.locator = ServiceLocator()
    if debug:
      self.apim = self.locator.getDebug()
      self.apia = self.locator.getAPIA()
    else:
      self.apim = self.locator.getAPIM(host='localhost',port='8443',SSL=True,debug=True)
      self.apia = self.locator.getAPIA(host='localhost',port='8443',SSL=True,debug=True)
  def setBasicAuth(self, username, password):
    self.apim.binding.setAuth(username=username, password=password)
    self.apia.binding.setAuth(username=username, password=password)
  def setSSL(self, useSSL):
    self.apim.binding.setSSL(useSSL)
  def nextPid(self, pidNamespace='ldpd'):
    request = getNextPIDRequest(numPIDs=1, pidNamespace=pidNamespace)
    response = self.apim.getNextPID(request)
    return response.pid[0]

  def createStubObject(self,dateCreated):
    pid = self.nextPid()
    src = STUB_TEMPLATE.format({'pid':pid,'dateCreated':dateCreated})
    request = ingestRequest(objectXml=src)
    response = self.apim.ingest(request)
    pid = response.objectPID
    if not pid.startswith('info:fedora/'):
      pid = 'info:fedora/' + pid
    return pid
    
  def searchByDC(self, searchField, value, resultFields, verify):
    uris = []
    if not 'pid' in resultFields: resultFields.append('pid')
    request = findObjectsRequest(resultFields=resultFields, maxResults=2, query=[condition(searchField,"has",value)])
    response = self.apia.findObjects(request)
    for result in response.resultList:
      if verify and searchField in resultFields:
        rValues = result.__getattr__(searchField)
        if not(value in rValues):
          print value + " not in " + repr(rValues)
          raise Exception("false positive on dc:" + searchField + "value match: " + rValues[0] + " for " + value)
      objuri = 'info:fedora/' + result.pid[0]
      print 'found object ' + objuri + ' for ' + searchField + ' ' + value
      if objuri not in uris:
        uris.append(objuri)
    return uris
# do RI query (or admin query)
# if none returned, create stub object and return new pid
# else return found pid
  def getObjectForId(self, values, dateCreated, field='identifier'):
    uris = []
    for value in values:
      value = value.strip()
      if value.startswith('info:fedora/') and not value in self.map:
        self.map[value] = value
      if (value in self.map and self.map[value] not in uris):
        uris.append(self.map[value])
      else:
        resultList = self.searchByDC(searchField=field, value=value,resultFields=["pid","identifier"],verify=True)
        for result in resultList:
          self.map[value] = result
          print 'found object ' + self.map[value] + ' for ' + field + ' ' + value
          if self.map[value] not in uris:
            uris.append(self.map[value])
    if len(uris) > 1:
      print 'ERROR: ' + repr(uris)
    if len(uris) == 0:
      uris.append(self.createStubObject(dateCreated))
      print uris[0] + ' stub object created'
      for value in values:
        self.map[value] = uris[0]
        print uris[0] + ' created for ' + value
    if len(uris) == 1:
      for value in values:
        if not (value in self.map):
          self.map[value] = uris[0]
          print uris[0] + ' mapped to ' + value
    return self.map[values[0]]

  def getUriForId(self,identifiers):
    return self.map[identifiers[0].strip()]

  def getRelationshipXML(self, objuri, predicate, object, object_type=None):
    xml = ""
    object = object.strip()
    if (object in self.map):
      object = self.map[object]
    if (testUri(object) and object_type == None):
      isLiteral = False
    else:
      isLiteral = True
    if (objuri == object):
      print 'FAILURE: Not adding reflexive relationship ' + objuri + ' ' + predicate + ' ' + object
      return ""
    ri = max((predicate.rfind('#'),predicate.rfind('/')))
    ns = predicate[0:ri+1]
    ename = predicate[ri+1:len(predicate)]
    if isLiteral:
      xml =  '<' + ename + ' xmlns="' + ns + '">' + object + '</' + ename + '>'
    else:
      xml = '<' + ename + ' xmlns="' + ns + '" rdf:resource="' + object + '" />'
    return xml
  def addRelationship(self,objuri, predicate, object, object_type=None):
    object = object.strip()
    if (object in self.map):
      object = self.map[object]
    if (testUri(object) and object_type == None):
      isLiteral = False
    else:
      isLiteral = True
    if (objuri == object):
      print 'FAILURE: Not adding reflexive relationship ' + objuri + ' ' + predicate + ' ' + object
      return
    split = objuri.split('/')
    pid = split[-1]
    request = addRelationshipRequest(pid=pid, relationship=predicate, object=object, datatype=object_type, isLiteral=isLiteral)
    response = self.apim.addRelationship(request)
    if (response.added):
      print 'SUCCESS: added ' + objuri + ' ' + predicate + ' ' + object
    else:
      if (response.status is 200):
        print 'NOOP: existing triple at ' + objuri + ' ' + predicate + ' ' + object
      else:
        print 'FAILURE: could not add ' + objuri + ' ' + predicate + ' ' + object + ' status ' + str(response.status)
  def prepareDSLocation(self, oldDsLocation):
    newDsLocation = oldDsLocation.replace('/fstore/diskonly/','/fstore/archive/')
    if newDsLocation == oldDsLocation: return oldDsLocation
    newPath = newDsLocation.replace('file://','')
    oldPath = oldDsLocation.replace('file://','')
    (dir, file) = os.path.split(newPath)
    if not os.access(dir,os.F_OK): os.makedirs(dir)
    try:
      if (not self.hashEquals(oldPath, newPath)): shutil.copyfile(oldPath, newPath)
    except Exception as e:
      print 'ERROR: copying ' + oldPath + ' to ' + newPath + ': ' + repr(e)
    return newDsLocation
  def getChecksumInfo(self, dsLocation):
    if dsLocation.startswith('file://'):
      path = dsLocation.replace('file://','')
      md5Path = path + '.md5'
      if os.access(md5Path, os.R_OK):
        checksum = None
        csType = 'MD5'
        with open(md5Path) as f:
          checksum = f.read().strip()
        return (csType,checksum)
    return (None, None)
  def addDatastream(self, objuri, properties):
    if not("formatURI" in properties):
      properties["formatURI"] = None
    split = objuri.split('/')
    pid = split[-1]
    request = listDatastreamsRequest(pid=pid)
    response = self.apia.listDatastreams(request)
    found = False
    for datastream in response.datastreamDef:
      if datastream.ID == properties["dsID"]:
        found = True
    if not found:
      rclass = addDatastreamRequest
      op = self.apim.addDatastream
    else:
      rclass = modifyDatastreamByReferenceRequest
      op = self.apim.modifyDatastreamByReference
    (csType, cs) = self.getChecksumInfo(properties["dsLocation"])
    if not( "M" == properties["controlGroup"]):
      oldDsLocation = properties["dsLocation"]
      dsLocation = self.prepareDSLocation(oldDsLocation)
      properties["dsLocation"] = dsLocation

    if not "MIMEType" in properties:
      properties["MIMEType"] = 'binary/octet-stream'
    request = rclass(pid=pid, dsID=properties["dsID"],
                                     dsLocation=properties["dsLocation"],controlGroup=properties["controlGroup"],
                                     dsState=properties["dsState"],formatURI=properties["formatURI"],
                                     checksumType=csType,checksum=cs,MIMEType=properties["MIMEType"])
    try:
      response = op(request)
      if (response.status == 200):
        print 'SUCCESS: ' + str(op.__class__) +'  ' + objuri + '/' + properties["dsID"] + '=' + properties["dsLocation"]
        return True
      else:
        print 'ERROR: ' + str(op.__class__) +'  ' + objuri + '/' + properties["dsID"] + '=' + properties["dsLocation"]
        return False
    except Exception as ex:
      print 'ERROR: ' + repr(ex)
      print 'ERROR: ' + str(op.__class__) +'  ' + objuri + ' ' + repr(request)
      return False
  def addDcProps(self, objuri, properties):
    if (len(properties) == 0):
      return True
    value = U''
    for property in properties:
      vals = properties[property]
      ot = U'<dc:' + property + '>'
      ct = U'</dc:' + property + '>'
      for val in vals:
        val = val.replace('&','&amp;')
        value += (ot + val + ct)
        try:
          print objuri + ' dc:' + property + ' ' + val
        except UnicodeEncodeError as e:
          print objuri + ' dc:' + property + ' ' + "".join(map(lambda c: chr(min(ord(c),126)), val))
    dsContent = DC_TEMPLATE.format({"dc":value})
    try:
      print dsContent
    except UnicodeEncodeError as e:
      print "".join(map(lambda c: chr(min(ord(c),126)), dsContent))
    split = objuri.split('/')
    pid = split[-1]
    request = modifyDatastreamByValueRequest(pid=pid, dsID='DC',
                                             formatURI='http://www.openarchives.org/OAI/2.0/oai_dc/',
                                             dsContent=dsContent, dsLabel='Dublin Core Record for this object',
                                             MIMEType='text/xml')
    response = self.apim.modifyDatastreamByValue(request)
    print objuri + ' modified DC datastream by value at ' + str(response.modifiedDate)
  def addRelsDS(self, objuri, rels):
    # RELS
    rdfdata = U'<rdf:Description rdf:about="' + objuri + U'">'
    for rel in obj["RELSEXT"]:
      rdfdata += myApim.getRelationshipXML(objuri, rel["predicate"], rel["object"])
    rdfdata += U'</rdf:Description>'
    dsContent = RELS_TEMPLATE.format({"rdf":rdfdata})
    print dsContent
    split = objuri.split('/')
    pid = split[-1]
    request = modifyDatastreamByValueRequest(pid=pid, dsID='RELS-EXT',
                                             formatURI='info:fedora/fedora-system:FedoraRELSExt-1.0',
                                             dsContent=dsContent, dsLabel='RDF Statements about this object',
                                             MIMEType='application/rdf+xml')
    response = self.apim.modifyDatastreamByValue(request)
    print objuri + ' modified RELS-EXT datastream by value at ' + str(response.modifiedDate)
  def modifyObject(self, objuri, **kw):
    split = objuri.split('/')
    pid = split[-1]
    if 'label' not in kw:
      apiaRequest = getObjectProfileRequest(pid=pid, **kw)
      apiaResponse = self.apia.getObjectProfile(apisRequest)
      kw['label'] = apiaResponse.objLabel
    request = modifyObjectRequest(pid=pid, **kw)
    response = self.apim.modifyObject(request)

  def hashEquals(self, pathA, pathB):
    if (pathA.strip() == pathB.strip()): return True
    a = open(pathA, 'r', 8196)
    b = open(pathB, 'r', 8196)
    aHash = hashlib.md5()
    buffer = True
    while (buffer):
      buffer = a.read(8196)
      aHash.update(buffer)
    a.close()
    bHash = hashlib.md5()
    buffer = True
    while (buffer):
      buffer = b.read(8196)
      bHash.update(buffer)
    b.close()
    return (aHash.hexdigest() == bHash.hexdigest())
if __name__ == '__main__':
  opts, args = getopt.getopt(sys.argv[1:],'i:o:u:p:c:s:hw')
  jsonsrc = ''
  jsonfile=False
  user=False
  password=False
  useSSL = False
  collections = []
  overwrite = False
  usage = False
  for o,a in opts:
    if o == '-i':
      jsonsrc = a
      jsonfile = open(a, 'rb')
    if o == '-o':
      sys.stdout = open(a, 'w')
    if o == '-u':
      user = a
    if o == '-p':
      password = a
    if o == '-s':
      useSSl = a
    if o == '-w':
      overwrite = True
    if o == '-h':
      usage = True
    if o == '-c':
      for c in a.split(';'):
        collections.append(c)

  if usage or not jsonfile:
    print 'usage: python ' + sys.argv[0] + ' -i JSONFILE -u USER -p PASSWORD [-o OUTFILE][-c COLLECTION_ID][-s USESSL][-h PRINTHELP]'
    print 'JSONFILE : json input describing the load'
    print 'USER : fedora user'
    print 'PASSWORD : password for USER'
    print 'OUTFILE : File for log'
    print 'COLLECTION_ID : A semicolon-delimited list of pids or ids for collection'
    print 'USESSL : use ssl for API calls'
    exit()

  myApim = FedoraServices(debug=False)
  if (user):
    myApim.setBasicAuth(username=user,password=password)
  if (useSSL):
    myApim.setSSL(useSSL)
# map collections
  _collections = []
  for collection in collections:
    if collection.startswith('info:fedora/'):
      _collections.append(collection)
    else:
      _pids = myApim.searchByDC(searchField='identifier', value=collection,resultFields=["pid","identifier"],verify=True)
      for _pid in _pids:
        _collections.append(_pid)
  if len(_collections) != len(collections):
    print 'Could not map collections.'
    print 'collections: ' + repr(collections)
    print 'mapped: ' + repr(_collections)
    exit()
  collections = _collections
# load json
  _json = jsonfile.read()
  batch = json.loads(_json,"utf-8")
  objects = batch["objects"]
  now = datetime.datetime.now()
  timestamp = now.strftime("%Y-%m-%dT%H:%M:%S.000Z")
  jsonfile.close()
  for obj in objects:
    if not('dateCreated' in obj):
      obj['dateCreated'] = timestamp
    uri = myApim.getObjectForId(values=obj["DC"]["identifier"],dateCreated=obj['dateCreated'])
    obj['LOADEVENTS'] = []
    obj['LOADEVENTS'].append(uri + ' : ' + obj['dateCreated'] + ' : created for ' + repr(obj["DC"]["identifier"]))
  for obj in objects:
    uri = myApim.getUriForId(obj["DC"]["identifier"])
    print 'Got object ' + uri
    if (overwrite):
      # overwrite the datastream rather than submitting adds
      myApim.addRelsDS(uri,obj["RELSEXT"])
    else:
      for rel in obj["RELSEXT"]:
        myApim.addRelationship(uri,rel["predicate"],rel["object"])
        obj['LOADEVENTS'].append(uri + ' : ' + timestamp + ' : added relationship ' + rel["predicate"] + ' ' + rel["object"])
    if "DC" in obj:
      myApim.addDcProps(uri, obj["DC"])
      for prop in obj["DC"]:
        obj['LOADEVENTS'].append(uri + ' : ' + timestamp + ' : added DC property ' + prop + ' ' + repr(obj["DC"][prop]))
      if "title" in obj["DC"]:
        try:
          title = obj["DC"]["title"][0]
          title.encode('utf-8')
        except UnicodeEncodeError as e:
          print "\\x" + " \\x".join(map(lambda c: hex(ord(c)),obj["DC"]["title"][0]))
          raise e
        myApim.modifyObject(uri,label=title)
        obj['LOADEVENTS'].append(uri + ' : ' + timestamp + ' : modified object label "' + repr(obj["DC"]["title"]) + '"')
    if "DATASTREAMS" in obj:
      for datastream in obj["DATASTREAMS"]:
        try:
          myApim.addDatastream(uri,datastream)
          obj['LOADEVENTS'].append(uri + ' : ' + timestamp + ' : add/modify datastream ' + datastream['dsID'] + ' from ' + datastream['dsLocation'])
          if (datastream['dsID'] == 'CONTENT' and datastream['dsLocation'].startswith('file:')):
            dsPath = datastream['dsLocation'].replace('file://','')
            extent = os.path.getsize(dsPath)
            myApim.addRelationship(uri,'http://purl.org/dc/terms/extent',str(extent))
        except Exception as ex:
          print 'ERROR: adding datastream to ' + uri + ' ' + repr(ex) 
    if "rootNode" in obj and obj["rootNode"]:
      for collection in collections:
        if (uri == collection): continue
        myApim.addRelationship(uri,'http://purl.oclc.org/NET/CUL/memberOf',collection)
        obj['LOADEVENTS'].append(uri + ' : ' + timestamp + ' : added relationship ' + 'http://purl.oclc.org/NET/CUL/memberOf' + ' ' + collection)
  (manifestname, ext) = os.path.splitext(jsonsrc)
  timestamp = now.strftime("%Y-%m-%dT%H%M%S")
  manifestname += '.' + timestamp + ext
  manifest = open(manifestname, 'w')
  manifest.write(json.dumps(batch,indent=4))
  manifest.flush()
  manifest.close()
  sys.stdout.flush()
