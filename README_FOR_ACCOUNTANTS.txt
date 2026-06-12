# 💰 Crypto Wallet Analyzer
## For Accountants - Simple Usage Guide

---

## ✅ WHAT YOU HAVE

A single file: **`CryptoAnalyzer.exe`**

That's all you need! No installation, no Python, no setup.

---

## 🚀 STEP 1: Run the Program

**Double-click** `CryptoAnalyzer.exe`

A window appears with options.

---

## 🔑 STEP 2: Enter API Keys (First Time Only)

1. Click **⚙️ Set API Keys** button
2. You'll need 2 API keys:

### Get Alchemy Key (Free):
- Go to: https://dashboard.alchemy.com
- Sign up (free)
- Create new app
- Copy the "API Key"
- Paste it in the box

### Get Etherscan Key (Free):
- Go to: https://etherscan.io/apis
- Sign up (free)
- Create new API key
- Copy it
- Paste it in the box

3. Click **💾 Save Keys**

**That's it!** You only do this ONCE. Keys are saved on your computer.

---

## 📁 STEP 3: Prepare Your Excel Files

Create a folder with your balance Excel files:

```
my_wallets/
├── balance_january.xlsx
├── balance_february.xlsx
└── balance_march.xlsx
```

**Required columns in Excel:**
- `ticker` - Cryptocurrency (SOL, ETH, BTC)
- `wallet` - Wallet address
- `balance_date` - Date (any format)

---

## 📊 STEP 4: Run Analysis

1. Open **CryptoAnalyzer.exe**
2. Click **📂 Browse** button
3. Select your `my_wallets` folder
4. Enter date: **YYYY-MM-DD** format
   - Example: `2024-12-05`
5. Click **🚀 Run Analysis**
6. Wait (30 seconds to 5 minutes depending on wallets)
7. See "SUCCESS!" message ✓

---

## 📄 STEP 5: Get Your Results

A new file appears in your folder:

```
my_wallets/
├── balance_january.xlsx
├── analysis_2024-12-05.csv  ← Your results!
└── ...
```

Open the `.csv` file with Excel to see:
- Wallet addresses
- Crypto amounts (SOL, ETH, etc.)
- USD prices on that date
- Total USD values

---

## ⚠️ TROUBLESHOOTING

### "API Keys Not Configured"
→ Click **⚙️ Set API Keys** first

### "No XLSX Files Found"
→ Check:
  - Files are in selected folder
  - Files end with `.xlsx` (not `.xls`)
  - Excel files are closed

### "Invalid Date"
→ Use format: `2024-12-05` (not `12/05/2024`)

### Program takes a long time
→ Normal! Checking blockchain data takes 5-30 seconds per wallet

### "Wallet not found" or API errors
→ Check:
  - Wallet addresses are correct
  - Internet connection is working
  - Wallet has transaction history

---

## 🔐 SECURITY & PRIVACY

- ✓ Everything runs on YOUR computer
- ✓ No data sent to our servers
- ✓ Your wallet data stays private
- ✓ API keys stored only on your computer

---

## 📋 SUPPORTED CRYPTOCURRENCIES

| Crypto | Status |
|--------|--------|
| SOL (Solana) | ✅ Full support |
| ETH (Ethereum) | ✅ Full support |
| BTC (Bitcoin) | ⏳ Coming soon |

---

## 💡 TIPS FOR ACCOUNTANTS

1. **Keep backups** of your Excel files
2. **Use consistent wallet addresses** across files
3. **Historical dates** - You can analyze any past date
4. **Batch processing** - Analyze multiple wallets at once
5. **CSV format** - Open with Excel, Google Sheets, or any accounting software

---

## 📞 SUPPORT

**Issue**: Program crashes
→ Make sure you have latest Windows 10/11

**Issue**: Can't get API keys
→ Google "Alchemy API key" or "Etherscan API key" for videos

**Issue**: Something else
→ Contact: [Your email]

---

## ✨ THAT'S IT!

You're now analyzing crypto wallets like a pro! 🎉

No coding, no Python, no command line.

Just double-click and go! 💰
