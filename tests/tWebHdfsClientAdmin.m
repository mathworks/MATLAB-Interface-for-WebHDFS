classdef tWebHdfsClientAdmin < matlab.unittest.TestCase
    %
    % Copyright 2020-2021 The MathWorks, Inc.
    %
    properties
        cl
    end
    
    methods (TestClassSetup)
        
        function createDataAdmin(tc)
            tc.cl = WebHdfsClient('root','data/lab/dlb_ecb_public', 'name','maria_dev', 'protocol','http', 'host', "sandbox-hdp.hortonworks.com", "port", 50070);
        end
        
    end
    
    methods (Test)
        
        function tCheckACLStatus(tc)
            
            file = 'db/ecad_data_dp/myTT.csv';
            acl = tc.cl.hdfs_acl_status('db/ecad_data_dp/myTT.csv');
            
            tc.cl.hdfs_set_permission(file, '777');
            aclNew = tc.cl.hdfs_acl_status('db/ecad_data_dp/myTT.csv');
            tc.verifyEqual(aclNew.permission, '777');
            
            tc.cl.hdfs_set_permission(file, acl.permission);
            aclNew = tc.cl.hdfs_acl_status('./db/ecad_data_dp/myTT.csv');
            tc.verifyEqual(aclNew.permission, acl.permission);
            
        end
        
        
        
        function tUploadFileToDrirectoryWithPermission(tc)
            
            import matlab.unittest.constraints.IsFile;
            
            filename = 'myTestFile.txt';
            
            fx = matlab.unittest.fixtures.TemporaryFolderFixture();
            tc.applyFixture(fx)
            
            myFile = fullfile(fx.Folder, filename);
            
            fid = fopen( myFile, 'w');
            fwrite(fid, 'This is a MW Test');
            fclose(fid);
                        
            tc.cl.hdfs_upload( 'share' , myFile, overwrite=true, permission=777 );
            
            acl = tc.cl.hdfs_acl_status('db/ecad_data_dp/myTT.csv');
            tc.verifyEqual(acl.permission, '777');
            
            newFile = fullfile('share',filename);
            
            value = tc.cl.hdfs_exists(newFile);
            tc.verifyTrue(value);
            
            tc.cl.hdfs_delete(newFile);
            
            value = tc.cl.hdfs_exists(newFile);
            tc.verifyFalse(value);            
            
        end
        
        function tPreferences(tc)
            tc.cl = WebHdfsClient('root','data/lab/dlb_ecb_public', 'name','maria_dev', 'protocol','http', 'host', "sandbox-hdp.hortonworks.com", "port", 50070);
            tc.cl.saveConnectionDetails();
            tc.verifyEqual(getpref('webhdfs','port'), int32(50070))
            tc.verifyEqual(getpref('webhdfs','protocol'), 'http')
            tc.verifyEqual(getpref('webhdfs','host'), 'sandbox-hdp.hortonworks.com')
            
            newClient = WebHdfsClient('root','data/lab/dlb_ecb_public', 'name','maria_dev');
            tc.verifyEqual(newClient.WebHDFSConnection.Host, "sandbox-hdp.hortonworks.com")
            tc.verifyEqual(newClient.WebHDFSConnection.Protocol, "http")
            tc.verifyEqual(newClient.WebHDFSConnection.Port, int32(50070))
            
            tc.cl.clearConnectionDetails();
            tc.verifyFalse(ispref('webhdfs','host'))
            tc.verifyFalse(ispref('webhdfs','protocol'))
            tc.verifyFalse(ispref('webhdfs','port'))
            
            tc.cl.saveConnectionDetails();
        end
        
        function tEmptyHost(tc)
            
            tc.cl.clearConnectionDetails();            
            makeClient = @() WebHdfsClient('root','data/lab/dlb_ecb_public', 'name','maria_dev');            
            tc.verifyError(makeClient, 'webhdfs:WebHdfsConnection:emptyHost')
            
        end
        
    end
    
end