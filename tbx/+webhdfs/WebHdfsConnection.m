classdef WebHdfsConnection
    %
    % Copyright 2020-2021 The MathWorks, Inc.
    %
    properties (SetAccess = private)
        Host     (1,1) string = ""
        Port     (1,1) int32  = 50070
        Auth     (1,1) string
        Protocol (1,1) string = "https"
    end
    
    methods (Access = ?WebHdfsClient)
        
        function obj = WebHdfsConnection( varargin )
            
            % Get stored preferences if any
            if ispref('webhdfs','host')
               obj.Host = getpref('webhdfs','host'); 
            end
            
            if ispref('webhdfs','port')
               obj.Port = getpref('webhdfs','port'); 
            end
            
            if ispref('webhdfs','protocol')
               obj.Protocol = getpref('webhdfs','protocol'); 
            end
            
            p = inputParser();
            p.KeepUnmatched=true;
            validateText = @(x) ischar(x) | isstring(x);
            addParameter(p,'name','', validateText)
            addParameter(p,'host',obj.Host, validateText)
            addParameter(p,'port',obj.Port, @isnumeric)
            addParameter(p,'protocol',obj.Protocol, validateText)
            
            parse(p, varargin{:})
            
            if isempty(p.Results.name)
                obj.Auth = "op=";
            else
                obj.Auth = "user.name=" + p.Results.name + "&op=";
            end
            
            obj.Host     = p.Results.host;
            obj.Port     = p.Results.port;
            obj.Protocol = p.Results.protocol;
            
            % Check that a host has been provided, error otherwise
            if isequal(obj.Host, "")
                error('webhdfs:WebHdfsConnection:emptyHost', ...
                    'You must specify the hostname of your Hadoop cluster either during the construction of the class, or as a MATLAB prefernce setpref(''webhdfs'',''host'',<hostname>)')
            end
        end
        
        function savePrefs( obj )
            setpref('webhdfs','port',obj.Port)
            setpref('webhdfs','protocol', obj.Protocol)
            setpref('webhdfs','host', obj.Host)
        end
        
    end
    
    methods (Static)        
        
        function clearPrefs()
            if ispref('webhdfs')
                rmpref('webhdfs')
            end
        end
        
    end
    
    methods
        function AclStatus = acl_status(obj, hdfs_path, strict)
            
            narginchk(2,3)
            
            if nargin < 3
                strict = false;
            end
            
            try
                
                value = obj.readWebData('GETACLSTATUS', 'get', hdfs_path);
                AclStatus = value.AclStatus;
                
            catch err
                
                if strcmpi(err.identifier,'MATLAB:webservices:HTTP404StatusCodeError') && strict == false
                    AclStatus = struct.empty();
                else
                    rethrow(err)
                end
                
            end
        end
        
        function FileStatus = getFileStatus(obj, path, strict)
            
            try
                
                value = obj.readWebData('GETFILESTATUS', 'get', path);
                FileStatus = value.FileStatus;
                
            catch err
                
                if strcmpi(err.identifier,'MATLAB:webservices:HTTP404StatusCodeError') && strict == false
                    FileStatus = struct.empty();
                else
                    rethrow(err)
                end
                
            end
            
        end
        
        function FileStatuses = listDirectory(obj,path)
            
            value = obj.readWebData('LISTSTATUS', 'get', path);
            FileStatuses = value.FileStatuses.FileStatus;
            
            % If returned structs have different fields.
            if iscell(FileStatuses)
                FileStatuses = webhdfs.utils.mergeStructs(FileStatuses{:});
            end
            
        end
        
        function content = getContentSummary(obj,path, strict)
            
            try
                value = obj.readWebData('GETCONTENTSUMMARY', 'get', path);
                content = value.ContentSummary;
            catch err
                if strcmpi(err.identifier,'MATLAB:webservices:HTTP404StatusCodeError') && strict == false
                    content = struct.empty();
                else
                    rethrow(err)
                end
            end
            
        end
        
        function path = download(obj, hdfspath, localpath, overwrite)
            % Save file in "hdfspath" as "filename"
            
            FileStatus = obj.getFileStatus(hdfspath, false);
            
            if isempty(FileStatus)
                error('WebHDFSClient:download:unexistingFile','The specified HDFS file does not exist')
            end
            
            [filepath, name, ext] = fileparts(char(localpath));
            [~,hdfs_name, hdfs_ext] = fileparts(char(hdfspath));
            
            if strcmpi(FileStatus.type, 'FILE')
                
                if isempty(ext)
                    filepath = fullfile(filepath, name);
                    localpath = fullfile(filepath,  [hdfs_name, hdfs_ext]);
                end
                
                if ~isempty(filepath) && exist(filepath,'dir') == 0
                    mkdir(filepath)
                end
                
                path = obj.downloadFile(hdfspath, localpath, overwrite);
                
            elseif strcmpi(FileStatus.type, 'DIRECTORY')
                
                % Check if the local directory exists
                if ~isempty(localpath) && exist(localpath,'dir') == 0
                    mkdir(localpath)
                end
                
                documents = obj.listDirectory(hdfspath);
                
                for i = 1 : length(documents)
                    newPath = documents(i).pathSuffix;
                    obj.download( ...
                        fullfile(hdfspath, newPath), ...
                        fullfile(localpath, hdfs_name, hdfs_ext, newPath), overwrite );
                end
                
                path = fullfile(localpath, hdfs_name, hdfs_ext);
                
            else
                error('WebHDFSClient:download:HDFSPathIsNotValid','The supplied path does not point to a valid download entity')
            end
        end
        
        function response = deleteFile(obj, hdfspath, recursive)
            
            if recursive
                op = "DELETE&recursive=true";
            else
                op = "DELETE&recursive=false";
            end
            
            response = obj.writeWebData(op, 'delete', hdfspath);
            response = response.boolean;
           
        end
        
        function response = makeDirectory(obj, hdfspath)
            
            response = obj.writeWebData('MKDIRS', 'put', hdfspath);            
            response = response.boolean;
            
        end
        
        function response = renameFile(obj, hdfspath_old, hdfspath_new)
            
            hdfspath_new = strrep(hdfspath_new, "\", "/");
            op = "RENAME&destination="+hdfspath_new;
            
            response = obj.writeWebData(op, 'put', hdfspath_old);
            response = response.boolean;
        end
        
        function uploadFile(obj, hdfs_path, local_path, overwrite, permissions)
            
            if isfolder(local_path)
                
                files = dir(fullfile(local_path,'**/*') );
                
                for f = files'
                    if ~f.isdir
                        newHdfsPath = fullfile(hdfs_path, extractAfter(f.folder, local_path), f.name);
                        obj.uploadFile( newHdfsPath, fullfile(f.folder, f.name), overwrite, permissions);
                    end
                end
                
            elseif isfile(local_path)
                
                fid = fopen(local_path, 'rb');
                data = fread(fid, Inf, 'uint8=>uint8');
                fclose(fid);
                
                obj.addNewFile(hdfs_path, data, overwrite, permissions);
                
            else
                error('WebHDFSClient:uploadFile:UnsupportedEntity', 'This is not a folder or a file')
            end
            
        end
        
        function response = addNewFile(obj, hdfs_path, data, overwrite, permissions)
            
            % Set permissions if those are passed, leave default behaviour
            % otherwise.
            if ~isempty(permissions)
                validChmod = webhdfs.utils.checkValidChmod(permissions);
                perm = sprintf('&permission=%s', validChmod);
            else
                perm = '';
            end
            
            if overwrite
                op = "CREATE&overwrite=true" + perm;
            else
                op = "CREATE&overwrite=false" + perm;
            end
            
            response = obj.writeWebData(op, 'put', hdfs_path, data);
            
        end
        
        function response = appendToFile(obj, hdfs_path, data)
            
            response = obj.writeWebData("APPEND", 'post', hdfs_path, data);
            
        end
        
        function response = openFile(obj, hdfs_path, varargin)
            
            FileStatus = obj.getFileStatus(hdfs_path, true);
            
            if strcmp(FileStatus.type, 'FILE')
                
                response = obj.readWebData('OPEN', 'get', ...
                    hdfs_path, varargin{:});
                
            end
            
        end
        
        function fileStruct = walk(obj, hdfs_path, depth)
            
            s = obj.getFileStatus(hdfs_path, false);
            
            if ~isempty(s)
                if strcmp(s.type, 'DIRECTORY')
                    fileStruct = doWalk(hdfs_path, depth);
                else
                    fileStruct = s;
                end
            else
                fileStruct = s;
            end
            
            function list = doWalk(hdfs_path, depth)
                
                fileList = obj.listDirectory(hdfs_path);
                
                if ~isempty(fileList)
                    
                    for i = 1 : length(fileList)
                        
                        if  strcmp(fileList(i).type , 'DIRECTORY')
                            
                            folderName =  fileList(i).pathSuffix;
                            newPath = fullfile(hdfs_path,folderName);
                            
                            if depth == 0
                                list.(genvarname(folderName)) = "DIRECTORY";
                            else
                                list.(genvarname(folderName)) = doWalk( newPath, depth - 1);
                            end
                            
                        elseif strcmp(fileList(i).type , 'FILE')
                            
                            [~, filename, ~] = fileparts(fileList(i).pathSuffix);
                            
                            list.(genvarname(filename)) = fileList(i);
                            
                        end
                        
                    end
                    
                else
                    list = struct.empty();
                end
                
            end
            
        end
        
        function value = setPermission(obj, hdfs_path, permission)
            
            validChmod = webhdfs.utils.checkValidChmod(permission);
            
            op = sprintf("SETPERMISSION&permission=%s", validChmod);
            value = obj.writeWebData(op, 'put', hdfs_path);
            
        end
        
        function value = changeOwner(obj, hdfs_path, owner, group)
            
            if ~isempty(owner)
                owner = sprintf('&owner=%s', owner);
            end
            
            if ~isempty(group)
                group = sprintf('&group=%s', group);
            end
            
            op = sprintf("SETOWNER%s%s", owner,group);
            
            value = obj.writeWebData(op, 'put', hdfs_path);
            
        end
        
    end
    
    methods (Access = private)
        
        function path = downloadFile(obj, hdfspath, localpath, overwrite)
            
            if exist(localpath, 'file') == 2 && overwrite == false
                warning( 'WebHDFSClient:downloadFile:LocalFileExists', ...
                    'File %s already exists, skipping download ...', localpath)
                path = localpath;
            else
                [url, opt] = getCallDetails(obj, 'OPEN', hdfspath, 'get');
                path = websave(localpath, url, opt);
            end
            
        end
        
        function response = writeWebData(obj, operation, method, path, data)
            
            narginchk(4, 5)
            if nargin < 5
                data = '';
            end
            
            [url, ~] = obj.getCallDetails(operation, path, method);
            
            % Options
            options = matlab.net.http.HTTPOptions();            
            % Header field
            h1 = matlab.net.http.field.ContentTypeField('application/octet-stream');            
            % Create request
            request = matlab.net.http.RequestMessage(method);
            request = request.addFields(h1);
            request.Body = matlab.net.http.MessageBody(data);
            
            % Set URL
            uri = matlab.net.URI(url);
            
            % Make Request
            warning('off','MATLAB:http:BodyExpectedFor')
            response = request.send(uri, options);
            warning('on','MATLAB:http:BodyExpectedFor')
            
            if floor(double(response.StatusCode)/100) == 2
                response = response.Body.Data;
            else
                try
                    msg = string(response.StatusLine) + " " + response.Body.Data.RemoteException.message;
                catch
                    msg = string(response.StatusLine);
                end
                error('webhdfs:WebHdfsConnection:FailedRequest', msg)
            end
            
        end
        
        function value = readWebData(obj, operation, method, path, varargin)
            
            [url, opt] = obj.getCallDetails(operation, path, method);
            if nargin > 4 && ~isempty(varargin{1})
                opt.ContentReader = varargin{1};
            end
            value = webread(url, varargin{2:end}, opt);
            
        end
        
        function [url, opt] = getCallDetails(obj, operation, path, method)
            
            path = strrep(path, "\", "/");
            
            opt = weboptions('RequestMethod', method);
            
            callType = obj.Auth + operation;
            url = sprintf(obj.Protocol + "://%s:%d/webhdfs/v1%s?%s", obj.Host, obj.Port, path, callType);
            
        end
        
        function value = getCallType(obj, operation)
            
            value = obj.Auth + operation;
            
        end
        
    end
    
end