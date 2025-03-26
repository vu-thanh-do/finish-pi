const express = require('express');
const router = express.Router();
const { handleComment } = require('../controllers/commentController');

router.post('/comment', handleComment);

module.exports = router; 