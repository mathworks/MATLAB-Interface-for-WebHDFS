function validChmod = checkValidChmod(permission)
%
% Copyright 2020-2021 The MathWorks, Inc.
%
if isnumeric(permission)
    permission = num2str(permission);
end

if isempty(regexp(permission,'^[0-7][0-7][0-7]$', 'ONCE'))
    error('webhdfs:WebHDFSClient:setPermission:invalidNumber', ...
        'This is not a valid permission, please input a valid chmod number as show here https://chmod-calculator.com/')
else
    validChmod = permission;
end

end