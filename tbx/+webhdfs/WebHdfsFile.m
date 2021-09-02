classdef WebHdfsFile < handle
    %
    % Copyright 2020-2021 The MathWorks, Inc.
    %
    properties
        encoding (1,1) string = 'utf-8';
    end
    
    properties (SetAccess = private)
        hdfs_path (1,1) string
        mode      (1,:) char = 'r'
    end
    
    properties (Access = private)
        client
        writeAccess (1,1) logical = false;
        fileExists  (1,1) logical = false;
        offset      (1,1) uint64  = 0;
    end
    
    methods
        
        function obj = WebHdfsFile(address, mode, client, access)
            
            obj.hdfs_path = address;
            obj.client = client;
            obj.mode = mode;
            obj.writeAccess = access;
            
            value = obj.client.getFileStatus(obj.hdfs_path, false);
            
            if ~isempty(value)
                obj.fileExists = true;
            elseif isempty(value) && contains(obj.mode, 'a')
                obj.client.addNewFile(obj.hdfs_path, '', true, []);
                obj.fileExists = true;
            end
            
        end
        
        function value = readline(obj, varargin)
            
            p = inputParser;
            addParameter(p, 'encoding', 'UTF-8')
            parse(p, varargin{:});
            
            if ~contains(obj.mode, {'r', '+'})
                error('WebHDFSFile:WrongPermissions', ...
                    'This file is not opened for reading mode')
            end
            
            len = 0;
            buffer = [];
            while true
                try 
                   newB = obj.client.openFile(obj.hdfs_path,function_handle.empty(), 'length', 1, 'offset', obj.offset + len);
                   buffer = [buffer,  newB]; %#ok<AGROW>
                   len = len + 1;
                   if newB == 10
                       break
                   end
                catch
                    break
                end
            end
            
            obj.offset = obj.offset + len;
            
            if contains(obj.mode, {'b'})
                value = buffer;
            else
                value = native2unicode(buffer,  p.Results.encoding);
            end
            
        end
        
        function value = read(obj, varargin)
            
            p = inputParser;
            addOptional(p, 'fcn', function_handle.empty())
            addParameter(p, 'encoding', 'UTF-8')
            parse(p, varargin);
            
            if ~contains(obj.mode, {'r', '+'})
                error('WebHDFSFile:WrongPermissions', ...
                    'This file is not opened for reading mode')
            end
            
            if isempty(p.Results.fcn)
                value = obj.client.openFile(obj.hdfs_path);
                if contains(obj.mode, {'b'})
                    value = value';
                else
                    value = native2unicode(value',  p.Results.encoding);                    
                end
            else
                value = obj.client.openFile(obj.hdfs_path, p.Results.fcn{1});
            end
            
        end
        
        function resetFile(obj)
            obj.offset = 0;
        end
        
        function writeMatrix(obj, A, varargin)
            
            if ischar(A)
                A = string(A);
            end
            
            T = table(A);
            obj.writeTable(T,'WriteVariableNames', false, 'WriteRowNames', false, varargin{:});
            
        end
        
        function writeCell(obj, C, varargin)
            T = table(C);
            obj.writeTable(T, 'WriteVariableNames', false, 'WriteRowNames', false, varargin{:});
        end
        
        function writeTable(obj, table, varargin)
            
            if contains(obj.mode, 'w')
                DefaultWriteVariableNames = true;
            elseif contains(obj.mode, 'a')
                DefaultWriteVariableNames = false;
            end
            
            p = inputParser;
            addParameter(p, 'encoding','UTF-8', @(x) isstring(x) | ischar(x));
            addParameter(p, 'WriteVariableNames', DefaultWriteVariableNames,  @islogical);
            addParameter(p, 'WriteRowNames', false, @islogical);
            
            parse(p, varargin{:})
            
            data = webhdfs.utils.writeTextFile(table, ...
                p.Results.WriteVariableNames, p.Results.WriteRowNames, p.Results.encoding);
            
            if contains(obj.mode, 'w')
                obj.client.addNewFile(obj.hdfs_path, data, true, []);
            elseif contains(obj.mode, 'a')
                obj.client.appendToFile(obj.hdfs_path, data);
            end
            
        end
        
        function write(obj, data)
            
            if ~obj.writeAccess && ~contains(obj.mode, {'w', 'a'})
                error('WebHDFSFile:WrongPermissions', ...
                    'This file is not opened for writting mode')
            end
            
            if contains(obj.mode, 'w')
                obj.client.addNewFile(obj.hdfs_path, data, true, []);
            elseif contains(obj.mode, 'a')
                if ~obj.fileExists
                    obj.client.addNewFile(obj.hdfs_path, data, true, []);
                    obj.fileExists = true;
                else
                    obj.client.appendToFile(obj.hdfs_path, data);
                end
            end
            
        end
        
        function close(obj)
            delete(obj)
        end
        
    end
    
end