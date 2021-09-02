classdef WebHdfsClient
    %WEBHDFSCLIENT Summary of this class goes here
    %   Detailed explanation goes here
    %
    % Copyright 2020-2021 The MathWorks, Inc.
    %
    properties (Hidden)
        WebHDFSConnection webhdfs.WebHdfsConnection
    end
    
    properties (SetAccess = private)
        Root (1,1) string = "" % Root folder of the connection
    end
    
    methods
        
        % Constructor
        function obj = WebHdfsClient(varargin)
            
            p = inputParser();
            addParameter(p,'root',"", @(x) ischar(x) | isstring(x))  
            p.KeepUnmatched=true;
            
            parse(p, varargin{:})
                        
            if ~startsWith(p.Results.root, ["/", "\"])
                 obj.Root = "/" +  p.Results.root;
            else
                 obj.Root = p.Results.root;
            end
            
            obj.WebHDFSConnection = webhdfs.WebHdfsConnection(varargin{:});            
            
        end
        
        function saveConnectionDetails(obj)
            % SAVECONNECTIONDETAILS save host, port, and protocol as MATLAB
            % preferences that persist across sessions.
            obj.WebHDFSConnection.savePrefs()
        end
        
        function clearConnectionDetails(obj)
            % CLEARCONNECTIONDETAILS clear all connection preferences
            obj.WebHDFSConnection.clearPrefs()
        end
        
        function content = hdfs_content(obj, hdfs_path, varargin)
            % HDFS_CONTENT Get content summary for hdfs path specified,
            % only folder, according to:
            % http://hadoop.apache.org/docs/r1.0.4/webhdfs.html#ContentSummary
            %
            %   CONTENT = HDFS_CONTENT(OBJ, HDFS_PATH, STRICT)
            %
            % Input Parameters
            % ----------
            % hdfs_path : str
            %     Path in hdfs, specified only as relative to
            %     /data/lab/<lab_name>
            % strict: logical
            %     If false, return struct.empty() rather than raise an exception if
            %     path does not exist
            % Returns
            % -------
            % struct
            %     Structure summarizing content, with most relevant fields
            %     'fileCount', 'directoryCount', 'spaceConsumed'
            
            narginchk(2,4)
            
            p = inputParser();
            addParameter(p,'strict',false,  @(x) islogical(x))            
            parse(p, varargin{:})
            
            hdfs_path = obj.resolve(hdfs_path);
            content = obj.WebHDFSConnection.getContentSummary(hdfs_path, p.Results.strict);
            
        end
        
        function path = hdfs_download(obj, hdfs_path, local_path, varargin)
            % HDFS_DOWNLOAD Download directories or files from data lab and
            % save locally.
            %
            %   PATH = HDFS_DOWNLOAD(OBJ, HDFS_PATH, LOCAL_PATH, OVERWRITE)
            %
            % Parameters
            % ----------
            % hdfs_path : str
            %     Path in hdfs, specified only as relative to /data/lab/<lab_name>. It can be
            %     a path to a single file or to an entire directory
            %
            % local_path: str
            %     Path to local folder to download directories or files to
            %
            % overwrite: false
            %     If the file exists, skip download unless overwrite =
            %     true;
            %
            % Returns
            % -------
            %     If successful, returns local download path.
            p = inputParser();
            addParameter(p,'overwrite',false,  @(x) islogical(x))            
            parse(p, varargin{:})
            
            hdfs_path = obj.resolve(hdfs_path);
            
            path = obj.WebHDFSConnection.download(hdfs_path, local_path, p.Results.overwrite);
            
        end
        
        function value = hdfs_exists(obj, hdfs_path)
            % HDFS_EXISTS Returns True is the hdfs path exists, False otherwise.
            %
            %   VALUE = HDFS_EXISTS(OBJ, HDFS_PATH)
            %
            % Input Parameters
            % ----------
            % hdfs_path : str
            %     Path in hdfs, can be absolute or relative to /data/lab/<lab_name>
            %
            % Returns
            % ----------
            % logical
            
            value = ~isempty(obj.hdfs_status(hdfs_path));
        end
        
        function list = hdfs_list(obj, hdfs_path, varargin)
            % HDFS_LIST List all files in hdfs path specified.
            %
            %   LIST = HDFS_LIST(OBJ, HDFS_PATH, STATUS)
            %
            % Input Parameters
            % ----------
            % hdfs_path : str
            %     Path in hdfs, only folder and specified only as relative to
            %     /data/lab/<lab_name>
            %
            % status: logical
            %     If False, return only the names of the files
            %
            % Returns
            % ----------
            % String array
            %     String array with all files on the hdfs path specified.
            %     Else default is return 0×0 empty string array
            
            narginchk(2,4)
            
            p = inputParser();
            addParameter(p,'status',false,  @(x) islogical(x))            
            parse(p, varargin{:})
            
            path = obj.resolve(hdfs_path);
            
            list = obj.WebHDFSConnection.listDirectory(path);
            
            if ~p.Results.status
                list = string({list(:).pathSuffix});
            end
        end
        
        function fileID = hdfs_open(obj, hdfs_path, mode)
            % HDFS_OPEN Open file from hdfs path. A WebHdfsClient can open
            % the file only in "read" (r) mode. A DataLabContributor, or
            % DataLabAdmin can also open the files in "write" (w) or
            % "append" (a) modes.
            %
            %   FILEID = HDFS_OPEN(OBJ, HDFS_PATH, MODE)
            %
            % Parameters
            % ----------
            % hdfs_path : str
            %     Path in hdfs, specified only as relative to /data/lab/<lab_name>
            % mode : str
            %     'r','rt','rb'
            %     'w','wt','wb'
            %     'a','at','ab'
            % Returns
            % -------
            % File object
            
            narginchk(2, 3)
            
            writeAccess = false;
            
            if nargin < 3
                mode = 'r';
            end
            
            if contains(mode,'r')
                
                if ~obj.hdfs_exists(hdfs_path)
                    error('WebHdfsClient:hdfs_open:FileDoesNotExist', ...
                        "The file you are trying to read does not exist in Hadoop. If you want to create it, please open the file with write permissiosn (w/w+).\nFor more information, please run >>doc WebHdfsClient.hdfs_open"')
                end
                
            end
            
            if ~contains(mode, 'r')
                writeAccess = true;
            end
            
            hdfs_path = obj.resolve(hdfs_path);
            fileID = webhdfs.WebHdfsFile(hdfs_path, mode, obj.WebHDFSConnection, writeAccess);
            
        end
        
        function recentFiles = hdfs_recent_files(obj, hdfs_path, nfiles)
            % HDFS_RECENT_FILES Find most recently modified files in a directory
            %
            %   RECENTFILES = HDFS_RECENT_FILES(OBJ, HDFS_PATH, NFILES)
            %
            % Parameters
            % ----------
            % hdfs_path : str
            %    Path in hdfs, specified only as relative to /data/lab/<lab_name>
            %
            % nfiles: int
            %     Maximum number of files returned, default is 1.
            %
            % Returns
            % -------
            % List of length nfiles containing filenames
            
            narginchk(2,3)
            
            if nargin < 3
                nfiles = 1;
            end
            
            paths = obj.hdfs_list( hdfs_path, 'status', true);
            filePaths = paths(arrayfun(@(x) strcmpi(x.type,'file'),paths));
            
            if ~isempty(filePaths)
                [~,idx] = sort([filePaths.modificationTime], 'descend');
                
                fileNum = min(length(idx), nfiles);
                recentFiles = filePaths( idx(1:fileNum) );
            else
                recentFiles = [];
            end
            
        end
        
        function status = hdfs_status(obj, hdfs_path, varargin)
            % HDFS_STATUS Check status of hdfs path specified
            %
            %   STATUS = HDFS_STATUS(OBJ, HDFS_PATH, STRICT)
            %
            % Input Parameters
            % ----------
            % hdfs_path : str
            %     Path in hdfs, can be absolute or relative to /data/lab/<lab_name>
            %
            % strict: logical
            %     If False, return [] rather than raise an exception if path does not exist
            %
            % Returns
            % -------
            % struct
            %    Structure with information on the hdfs path specified.
            
            narginchk(2,4)
            
            p = inputParser();
            addParameter(p,'strict',false,  @(x) islogical(x))            
            parse(p, varargin{:})
            
            hdfs_path = obj.resolve(hdfs_path);
            
            status = obj.WebHDFSConnection.getFileStatus(hdfs_path, p.Results.strict);
            
        end
        
        function fileStructure = hdfs_walk(obj, hdfs_path, depth)
            % HDFS_WALK  Depth-first walk of remote filesystem
            %
            %   FILESTRUCTURE = HDFS_WALK(OBJ, HDFS_PATH, DEPTH)
            %
            % Parameters
            % ----------
            % hdfs_path : str
            %     Path in hdfs, specified only as relative to /data/lab/<lab_name>
            %
            % depth: int
            %     Maximum depth to explore. 0 for no limit.
            %
            % Returns
            % -------
            % struct
            %     Structure with information on the contents of each level
            %     on the filesystem up until the level specified in depth.
            
            narginchk(2,3)
            
            if nargin < 3
                depth = 5;
            end
            
            hdfs_path = obj.resolve(hdfs_path);
            fileStructure = obj.WebHDFSConnection.walk(hdfs_path, depth);
            
        end
        
        function response = hdfs_delete(obj, hdfs_path, varargin)
            % HDFS_DELETE Remove file or directory from hdfs
            %
            %   RESPONSE = HDFS_DELETE(OBJ,HDFS_PATH, RECURSIVE) 
            %
            % Parameters
            % ----------
            % hdfs_path : str
            %     Path in hdfs, specified only as relative to /data/lab/<lab_name>. It can be
            %     a path to a single file or to an entire directory
            %
            % recursive: bool
            %     Delete content of non-empty directories recursively, default is False
            %
            % Returns
            % -------
            % bool
            %     True if successful, False if directory does not exist
            
            narginchk(2,4)
            
            p = inputParser();
            addParameter(p,'recursive',false,  @(x) islogical(x))            
            parse(p, varargin{:})
            
            hdfs_path = obj.resolve(hdfs_path);
            
            response = obj.WebHDFSConnection.deleteFile(hdfs_path, p.Results.recursive);
            
        end
        
        function hdfs_makedirs(obj, hdfs_path, varargin)
            % HDFS_MAKEDIRS Create directory in hdfs; recursively creates
            % intermediary directories if missing
            %
            %   HDFS_MAKEDIRS(OBJ,HDFS_PATH, OVERWRITE=<true/false>) 
            %
            % Parameters
            % ----------
            % hdfs_path : str
            %     Path in hdfs, specified only as relative to /data/lab/<lab_name>,
            %     containing directory to be created (and intermediary ones if applicable)
            narginchk(2,4)
            
            p = inputParser();
            addParameter(p,'overwrite',false,  @(x) islogical(x))            
            parse(p, varargin{:})
            
            path = obj.resolve(hdfs_path);
            
            if ~p.Results.overwrite && obj.hdfs_exists(hdfs_path)
                error('dlt:DataLabContributor:FileAlreadyExists', 'This file or folder already exists')
            end
            
            obj.WebHDFSConnection.makeDirectory(path);
            
        end
        
        function response = hdfs_rename(obj, hdfs_src_path, hdfs_dst_path)
            % HDFS_RENAME Move files or directories in hdfs. If
            % hdfs_dst_path is an existing directory, then the
            % file/directory in hdfs_src_path will be moved into it. If the
            % destination is an existing file or parent destination is
            % missing then raise an error.
            %
            %   RESPONSE = HDFS_RENAME(OBJ,HDFS_SRC_PATH, HDFS_DST_PATH) 
            %
            % Parameters
            % ----------
            % hdfs_src_path : str
            %     Path in hdfs, specified only as relative to /data/lab/<lab_name>. It can be
            %     a path to a single file or to an entire directory
            %
            % hdfs_dst_path: str
            %     Delete content of non-empty directories recursively, default is False
            
            response = obj.WebHDFSConnection.renameFile(obj.resolve(hdfs_src_path), obj.resolve(hdfs_dst_path));
        end
        
        function hdfs_upload(obj, hdfs_path, local_path, varargin)
            % HDFS_UPLOAD Upload local file or files in local folder to
            % the data lab.
            %
            %   HDFS_UPLOAD(OBJ,HDFS_PATH, LOCAL_PATH, OVERWRITE)
            %
            % Parameters
            % ----------
            % hdfs_path : str
            %     Path in hdfs, specified only as relative to /data/lab/<lab_name>.
            %
            % local_path: str
            %    Local path to folder or file
            %
            % overwrite: bool
            %     Overwrite file if exists, default is False
            %
            % permission: str/num
            %     Set chmod permissions for all the upload files. If not
            %     specified default HDFS settings are picked up.
            %
            % Returns
            % -------
            % str
            %     Full path to the target file/folder
            narginchk(3,7)
            
            p = inputParser();
            addParameter(p,'overwrite',false,  @(x) islogical(x))
            addParameter(p,'permission', []) 
            parse(p, varargin{:})
            
            status = obj.hdfs_status(hdfs_path, 'strict', false);
            
            hdfs_path = obj.resolve(hdfs_path);
            
            if ~isempty(status)
                
                if strcmpi(status.type, 'DIRECTORY')
                    
                    [~, name, ext] = fileparts(char(local_path));
                    hdfs_path = fullfile(hdfs_path, [name, ext]);
                    
                elseif strcmpi(status.type, 'FILE') && p.Results.overwrite == false
                    
                    error('DataLabContributor:FileAlreadyExists', ...
                        "File " + hdfs_path + "already exists")
                    
                end
                
            end
            
            obj.WebHDFSConnection.uploadFile(hdfs_path, local_path, p.Results.overwrite, p.Results.permission );
            
        end
        
        function response = hdfs_set_permission(obj, hdfs_path, permission)
            % HDFS_SET_PERMISSION Change the permissions of a file in hdfs
            %
            %   HDFS_SET_PERMISSION(OBJ, HDFS_PATH, PERMISSION)
            %
            % Parameters
            % ----------
            % hdfs_path : str
            %     Path in hdfs, specified only as relative to /data/lab/<lab_name>.
            %
            % permission: str
            %    File read/write/execute permissions for user/group/other
            %    in octal form
            %
            % Returns
            % -------
            % bool
            %      True if successful, false if change fails.
            hdfs_path = obj.resolve(hdfs_path);
            response = obj.WebHDFSConnection.setPermission(hdfs_path, permission );
            
            if isempty(response)
                response = true;
            else
                warning('webhdfs:WebHdfsClient:UnableToChangePermissions', ...
                    "Unable to change permissions, the reason is: " + response.StatusLine.string)                
                response = false;
            end
            
        end
        
        function status = hdfs_acl_status(obj, hdfs_path, varargin)
            
            narginchk(2,4)
            
            p = inputParser();
            addParameter(p,'strict',false,  @(x) islogical(x))            
            parse(p, varargin{:})
            
            hdfs_path = obj.resolve(hdfs_path);
            
            status = obj.WebHDFSConnection.acl_status(hdfs_path, p.Results.strict);
        end
        
        function status = hdfs_change_owner(obj, hdfs_path, varargin)
            
            p = inputParser;
            p.addParameter("owner",'')
            p.addParameter("group",'')
            parse(p, varargin{:});
            owner = p.Results.owner;
            group = p.Results.group;
            
            if nargin < 4
                group = [];
            end
                        
            hdfs_path = obj.resolve(hdfs_path);
            
            status = obj.WebHDFSConnection.changeOwner(hdfs_path, owner, group);
        end
    end
    
    methods (Access = private)
        
        function hdfs_path = resolve(obj, hdfs_path)
            
            if ~startsWith(hdfs_path, ["/", "\"])                
                hdfs_path = fullfile(obj.Root, hdfs_path);
            end
                        
            hdfs_path = strrep(hdfs_path, "\", "/");
            
        end
    end
    
end

