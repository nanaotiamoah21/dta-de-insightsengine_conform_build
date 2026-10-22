[v1.0.0] - Repo Creation - Initial Commit


# v[1.0.1](https://github.com/DigitalInnovation/dta-de-sst_template_repo/pull/3) (24-Sep-2026)
______________________________________________________
## 🐛 Bug Fix




<pre>
<table>
<tr>
<td>CRQ Number</td>
<td>DTRM00000000900</td>
</tr>
<tr>
<td>Domain Name</td>
<td>Shared</td>
</tr>
<tr>
<td>Process Name</td>
<td>CICD security fixes</td>
</tr>
<tr>
<td>Change Description</td>
<td>xRef-127 — Data holds 94 of the 156 open secret alerts.
Currently, our CICD is leaking Azure secrets in logs. This is considered a critical security vulnerability and we have been given a short time to remediate. This fix remdiates this issue and will be applied to every ETL repo.</td>
</tr>
<tr>
<td>Business Owner</td>
<td>NA</td>
</tr>
<tr>
<td>Tech Lead</td>
<td>Rhys Mansel</td>
</tr>
<tr>
<td>Approver Name</td>
<td>Dipanjan12,VJVimal</td>
</tr>
<tr>
<td>Business Go Live Date</td>
<td>24-Sep-2026</td>
</tr>
<tr>
<td>IT Go Live Date</td>
<td>24-Sep-2026</td>
</tr>
</table>
</pre>

🔰 Authors:

  RhysManselMNS 

📝 Documentation:

 - For more information please refer to design documentation 🔗NA

🔔 Detailed Commit Information: 

 .github/workflows/github_cicd_process.yml 
 notebooks/dummy_notebooks.py 
 schemas/dummy_schemas.json 

<br>
